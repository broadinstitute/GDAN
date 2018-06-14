#!/usr/bin/env python
import sys
import os
import logging
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from pkg_resources import resource_filename
from glob import glob
from io import open

from firecloud.fiss import main as call_fiss, _confirm_prompt as ask

def get_configs(workflow):
    with open(workflow, 'r') as configs:
        for line in configs:
            config = line.strip()
            if config.endswith(';') and '->' not in config:
                yield config.rstrip(';').strip('"')

def get_cohort(loadfile):
    return os.path.basename(loadfile).split('.', 1)[0]

def fissfc(*args):
    return call_fiss(["fissfc", "-V"] + list(args))

def main():
    logging.basicConfig(level=logging.INFO)
    
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,
                            description='''
    Create a new data standardization workspace, named stddata__YYYY_MM_DD.
    The space will be populated with dicer-normalized data and the stddata
    method configs, from which the sample set creation (merge) pipelines will
    be executed.''')
    parser.add_argument('-m', '--methods',
                        default='broad-firecloud-gdac/Production',
                        help='Production workspace containing method configs.')
    parser.add_argument('-p', '--project', default='broad-firecloud-gdac',
                        help='Project to create the stddata workspace in.')
    parser.add_argument('-n', '--namespace', default='broadgdac',
                        help='Method Config namespace.')
    parser.add_argument('-w', '--workflow',
                        default=resource_filename(__name__,
                                                  os.path.join('defaults',
                                                               'stddata.dot')),
                        help='DOT format file with stddata workflow.')
    parser.add_argument('-d', '--datestamp', default='latest',
                        help='Specify the dicing date to use in YYYY_MM_DD.')
    parser.add_argument('-r', '--recovery_file',
                        default=os.path.expanduser(os.path.join('~', '.fiss',
                                                                'stddata.json')),
                        help='''File to save monitor data. This file can be
                        passed to fissfc supervise_recover in case the
                        supervisor crashes.''')
    parser.add_argument('loadfile_root',
                        help='Path to the datestamped loadfile directories.')
    args = parser.parse_args()
    
    methods   = args.methods
    fromproject, fromspace = methods.split('/')
    toproject = args.project
    namespace = args.namespace
    workflow  = args.workflow
    recover   = args.recovery_file
    loadfiles = os.path.realpath(os.path.join(args.loadfile_root,
                                              args.datestamp))
    datestamp = os.path.basename(loadfiles)
    stddata   = 'stddata__' + datestamp
    
    if not os.path.isdir(loadfiles):
        logging.error('%s was not found, please check your datestamp and ' +
                      'loadfile_root arguments.', loadfiles)
        sys.exit(1)
    
    logging.info('Checking for %s/%s ...', toproject, stddata)
    if fissfc('space_exists', '-p', toproject, '-w', stddata):
        if ask('{}/{} already exists, delete it and continue'.format(toproject,
                                                                     stddata),
               prompt='? [Y\\n]: '):
            fissfc('-y', 'space_delete', '-p', toproject, '-w', stddata)
        else:
            logging.info('User chose not to delete existing space. Exiting.')
            sys.exit()
    
    # Create new workspace
    logging.info('Creating workspace %s/%s', toproject, stddata)
    fissfc('space_new', '-p', toproject, '-w', stddata)
    
    # Add broadgdac group as owner
    # TODO: make this configurable
    group='GROUP_broadgdac@firecloud.org'
    logging.info('Adding %s as workspace owner', group)
    fissfc('space_set_acl', '-p', toproject, '-w', stddata, '-r', 'OWNER',
           '--users', group)
    
    # Set workspace annotations
    logging.info('Setting data_version to ' + datestamp)
    fissfc('-y', 'attr_set', '-p', toproject, '-w', stddata,
           '-a', 'data_version', '-v', datestamp)
    
    logging.info('Setting package to "true"')
    fissfc('-y', 'attr_set', '-p', toproject, '-w', stddata, '-a', 'package',
           '-v', 'true')
    
    # Copy method configs
    logging.info('Copying method configs from %s to %s/%s', methods, toproject,
                 stddata)
    for config in get_configs(workflow):
        fissfc('config_copy', '-c', config, '-n', namespace, '-p', fromproject,
               '-s', fromspace, '-P', toproject, '-S', stddata)
    
    # Load loadfiles
    # Note: should globs for each type of loadfile be configurable?
    logging.info('Loading Participants')
    for participants in glob(os.path.join(loadfiles,
                                          '*-*.Participant.loadfile.txt')):
        logging.info('... from ' + get_cohort(participants))
        fissfc('entity_import', '-p', toproject, '-w', stddata,
               '-f', participants)
    
    logging.info('Loading Samples')
    for samples in glob(os.path.join(loadfiles, '*-*.Sample.loadfile.txt')):
        logging.info('... from ' + get_cohort(samples))
        fissfc('entity_import', '-p', toproject, '-w', stddata, '-f', samples)
    
    logging.info('Loading Sample_Sets')
    for sample_sets in glob(os.path.join(loadfiles,
                                         '*-*.Sample_Set.loadfile.txt')):
        logging.info('... from ' + get_cohort(sample_sets))
        fissfc('entity_import', '-p', toproject, '-w', stddata,
               '-f', sample_sets)
    
    # Initiate supervisor mode
    logging.info('Initiating stddata run. Recovery file is at:\n\t' + recover)
    fissfc('-y', 'supervise', '-p', toproject, '-w', stddata,
           '-n', namespace, '-j', recover, workflow)

if __name__ == '__main__':
    main()
