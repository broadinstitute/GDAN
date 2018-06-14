#!/usr/bin/env python

# TODO: use a base class for logging and base CLI arguments, put functions that
#       are applicable to other scripts into a common library.

import sys
import os
import logging
import re
import csv

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType, \
                     ArgumentTypeError
from pkg_resources import resource_filename

from firecloud.fiss import main as call_fiss, _confirm_prompt as ask
from firecloud.fccore import __fcconfig as fcconfig

def get_configs(workflow):
    for line in workflow:
        config = line.strip()
        if config.endswith(';') and '->' not in config:
            yield config.rstrip(';').strip('"')
    workflow.close()

def fissfc(*args):
    return call_fiss(["fissfc", "-V", "-y"] + list(args))

def analyses_sset_list(project, space, user_ssets=None):
    ssets = fissfc('sset_list', '-p', project, '-w', space)
    for sset in ssets:
        if user_ssets is not None:
            if sset in user_ssets:
                yield sset
        else:
            sset_lower = sset.lower()
            sset_cohort = sset_lower.split('-')[-2]
            if sset_cohort in ('stes', 'gbmlgg', 'kipan', 'pangi'):
                continue # Exclude aggregate cohorts other than coadread
            if sset_lower.endswith('laml-tb'):
                yield sset
            elif sset_lower.endswith('skcm-tm'):
                yield sset
            elif sset_lower.endswith("-tp") and sset_cohort not in ('skcm',
                                                                    'laml'):
                yield sset

def load_attributes(attributes):
    attr_reader = csv.DictReader(attributes, dialect='excel-tab')
    attr_dict = dict()
    for row in attr_reader:
        attr_dict[row[attr_reader.fieldnames[0]]] = \
                    {field: row[field] for field in attr_reader.fieldnames[1:]}
    attributes.close()
    return attr_dict

def valid_datestamp(datestamp):
    if re.match(r'^[2-9][0-9]{3}_[0-1][0-9]_[0-3][0-9]$', datestamp):
        return datestamp
    raise ArgumentTypeError("Malformed datestamp: {} (must be YYYY_MM_DD)".format(datestamp))

def remove_suffix(sset_name):
    """If more than one hyphen in sset_name, remove last hyphen and everything
    after it"""
    name_fields = sset_name.split('-')
    if len(name_fields > 2):
        return '-'.join(name_fields[:-1])
    return sset_name

def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s::%(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %I:%M:%S %p')

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,
                            description='''
    Create a new data analyses workspace, named analyses__YYYY_MM_DD.
    The space will be populated with the appropriate tumor subtypes from the
    associated stddata run and the analyses method configs, from which the
    sample set analysis pipelines will be executed.''')
    parser.add_argument('-d', '--datestamp', type=valid_datestamp,
                        required=True,
                        help='Specify the stddata date to use in YYYY_MM_DD.')
    parser.add_argument('-m', '--methods',
                        default='broad-firecloud-gdac/Production',
                        help='Production workspace containing method configs.')
    parser.add_argument('-p', '--from_project',
                        default=(fcconfig.project or 'broad-firecloud-gdac'),
                        help='Project the stddata workspace is in.')
    parser.add_argument('-P', '--to_project', default=(fcconfig.project or
                                                       'broad-firecloud-gdac'),
                        help='Project to create the analyses workspace in.')
    parser.add_argument('-n', '--namespace', default=(fcconfig.method_ns
                                                      or 'broadgdac'),
                        help='Method Config namespace.')
    parser.add_argument('-w', '--workflow', type=FileType('r'),
                        default=resource_filename(__name__,
                                                  os.path.join('defaults',
                                                               'Analyses.dot')),
                        help='DOT format file with analyses workflow.')
    parser.add_argument('-s', '--ssets', metavar='SSET', nargs='+',
                        help='Specific Sample Set(s) to use.')
    parser.add_argument('-a', '--attributes', type=FileType('r'),
                        default=resource_filename(__name__,
                                                  os.path.join('defaults',
                                                               'sample_set_loadfile.tsv')),
                        help='File of sample set attributes to add.')
    parser.add_argument('-r', '--recovery_file',
                        default=os.path.expanduser(os.path.join('~', '.fiss',
                                                                'Analyses.json')),
                        help='''File to save monitor data. This file can be
                        passed to fissfc supervise_recover in case the
                        supervisor crashes.''')
    
    args = parser.parse_args()
    methods     = args.methods
    methproject, methspace = methods.split('/')
    fromproject = args.from_project
    toproject   = args.to_project
    namespace   = args.namespace
    workflow    = args.workflow
    datestamp   = args.datestamp
    user_ssets  = args.ssets
    attributes  = args.attributes
    recover     = args.recovery_file
    stddata     = 'stddata__' + datestamp
    analyses    = '__' + datestamp
    if user_ssets is None:
        analyses = 'analyses' + analyses
    else:
        analyses = 'awg_{}{}'.format('_'.join(sorted(set(remove_suffix(sset).lower()
                                                         for sset in user_ssets))),
                                     analyses)

    if not fissfc("space_exists", '-p', fromproject, "-w", stddata):
        logging.error("Invalid workspace: {}/{} does not exist".format(fromproject,
                                                                       stddata))
        sys.exit(1)

    logging.info('Checking for {}/{} ...'.format(toproject, analyses))
    if fissfc('space_exists', '-p', toproject, '-w', analyses):
        if ask('{}/{} already exists, delete it and continue'.format(toproject,
                                                                     analyses),
               prompt='? [Y\\n]: '):
            fissfc('space_delete', '-p', toproject, '-w', analyses)
        else:
            logging.info('User chose not to delete existing space. Exiting.')
            sys.exit()

    # Create new workspace
    logging.info('Creating workspace {}/{}'.format(toproject, analyses))
    fissfc('space_new', '-p', toproject, '-w', analyses)

    # Add broadgdac group as owner
    # TODO: make this configurable
    group='GROUP_broadgdac@firecloud.org'
    logging.info('Adding {} as workspace owner'.format(group))
    fissfc('space_set_acl', '-p', toproject, '-w', analyses, '-r', 'OWNER',
           '--users', group)

    # Set workspace annotations
    logging.info('Setting data_version to ' + datestamp)
    fissfc('attr_set', '-p', toproject, '-w', analyses,
           '-a', 'data_version', '-v', datestamp)

    logging.info('Setting package to "true"')
    fissfc('attr_set', '-p', toproject, '-w', analyses, '-a', 'package',
           '-v', 'true')

    # Copy method configs
    logging.info('Copying method configs from {} to {}/{}'.format(methods,
                                                                  toproject,
                                                                  analyses))
    for config in get_configs(workflow):
        fissfc('config_copy', '-c', config, '-n', namespace, '-p', methproject,
               '-s', methspace, '-P', toproject, '-S', analyses)

    # Load optional attributes
    if attributes is not None:
        attributes = load_attributes(attributes)

    # Copy sample sets (and set attributes)
    logging.info('Copying sample sets from {}/{} to {}/{}'.format(fromproject,
                                                                  stddata,
                                                                  toproject,
                                                                  analyses))
    for sset in analyses_sset_list(fromproject, stddata, user_ssets):
        fissfc("entity_copy", "-t", "sample_set", "-e", sset,
               "-w", stddata, "-p", fromproject,
               "-W", analyses, "-P", toproject, "-l")
        if attributes is not None and sset in attributes:
            for attr, value in attributes[sset].items():
                fissfc("attr_set", "-t", "sample_set", "-e", sset, "-a", attr,
                       "-v", value, "-w", analyses, "-p", toproject)

    # Initiate supervisor mode
    fissfc('supervise', '-p', toproject, '-w', analyses,
           '-n', namespace, '-j', recover, workflow.name)

if __name__ == '__main__':
    main()
