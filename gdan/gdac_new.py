#! /usr/bin/env python
# encoding: utf-8
'''
Copyright (c) 2018 The Broad Institute, Inc. 
SOFTWARE COPYRIGHT NOTICE 
This software and its documentation are the copyright of the Broad Institute,
Inc. All rights are reserved.

This software is supplied without any warranty or guaranteed support
whatsoever. The Broad Institute is not responsible for its use, misuse, or
functionality.

@author: David Heiman
@date:   Aug 24, 2018
'''

import os
import sys
import logging
import subprocess
from argparse import ArgumentParser, FileType
from getpass import getuser
from pkg_resources import resource_filename
from firecloud.fiss import main as call_fiss, fcconfig, space_set_acl, _confirm_prompt as ask
from six.moves import input

def fissfc(*args):
    return call_fiss(["fissfc", "-V"] + list(args))

def get_configs(workflow):
    with open(workflow, 'r') as configs:
        for line in configs:
            config = line.strip()
            if config.endswith(';') and '->' not in config:
                yield config.rstrip(';').strip('"')

def create_workspace(project, workspace):
    username = getuser()
    logging.info('Checking for %s/%s ...', project, workspace)
    if fissfc('space_exists', '-p', project, '-w', workspace):
        if not workspace.endswith(username) and \
           ask('{}/{}'.format(project, workspace) + 'already exists, use ' +
               '{} instead'.format(workspace + '__' + username),
               prompt='? [y/N]: '):
            workspace = workspace + '__' + username
        else:
            workspace = input(workspace + ' already exists in ' + project +
                              '. Please create a unique workspace name: ')
        return create_workspace(project, workspace)
    
    # Create new workspace
    logging.info('Creating workspace %s/%s', project, workspace)
    fissfc('space_new', '-p', project, '-w', workspace)
    
    # Workspace name may have been modified, so return the final version
    return workspace

def set_acl(role, args):
    attr = getattr(args, role.lower() + 's')
    if attr:
        args.users = attr
        args.role = role.upper()
        logging.info('Adding %s as workspace %s(s)', ', '.join(attr), role)
        space_set_acl(args)

def main():
    
    parser = ArgumentParser(description="""
    Create given project/workspace. Loads default attributes and ACLs.
    Loads methods from specified DOT file. Loads entities from loadfiles
    in the working directory. Adds custom attributes""")
    parser.add_argument('-m', '--methods',
                        default='broad-firecloud-gdac/Production',
                        help='Production workspace containing method configs' +
                             ' (default: %(default)s).')
    parser.add_argument('-n', '--namespace', default='broadgdac',
                        help='Method Config namespace (default: %(default)s).')
    parser.add_argument('-o', '--owners', metavar='OWNER', nargs='*',
                        default=['GROUP_broadgdac@firecloud.org'],
                        help='Additional workspace owner(s).')
    parser.add_argument('-r', '--readers', metavar='READER', nargs='*',
                        default=['Getz_Lab@firecloud.org'],
                        help='Workspace reader(s) (default: %(default)s).')
    parser.add_argument('-w', '--writers', metavar='WRITER', nargs='*',
                        help='Workspace writer(s).')
    proj_kwargs = {'help': 'FireCloud Billing Project'}
    if fcconfig.project:
        proj_kwargs['help'] += ' (default: %(default)s).'
        proj_kwargs['default'] = fcconfig.project
    else:
        proj_kwargs['help'] += '.'
        proj_kwargs['required'] = True
    parser.add_argument('-p', '--project', **proj_kwargs)
    parser.add_argument('-s', '--workspace', help='Workspace for run. ' +
                        'Defaults to "awg_COHORT"')
    parser.add_argument('cohort', help='Name of the tumor cohort ' +
                        '(e.g. TCGA-LUAD, TCGA-STES, CPTAC3-UCEC, etc.). ' +
                        'This should match the root names of your loadfiles')
    parser.add_argument('-f', '--workflow',
                        default=resource_filename(__name__,
                                                  os.path.join('defaults',
                                                               'gdan.dot')),
                        help='DOT file specifying the workflow containing ' +
                             'the methods (default: %(default)s).')
    parser.add_argument('-a', '--attributes', type=FileType('r'),
                        default=resource_filename(__name__,
                                                  os.path.join('defaults',
                                                               'sample_set_loadfile.tsv')),
                        help='File of sample set attributes to add (default: %(default)s).')
    parser.add_argument('-d', '--dashboard', help='Generate dashboard for ' +
                        'run tracking', action='store_true')
    parser.add_argument('-l', '--logfile', help='Write logging output to file')
    args = parser.parse_args()
    
    log_kwargs = {'level'  : logging.INFO,
                  'format' : '%(levelname)-8s %(message)s'}
    if args.logfile is not None:
        log_kwargs['filename'] = args.logfile
        log_kwargs['format']   = '%(asctime)s::%(levelname)-8s %(message)s'
        log_kwargs['datefmt']  = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(**log_kwargs)
    # Confirm loadfiles exist before going any further
    loadfiles = dict(zip(['participants', 'samples', 'sample_sets'],
                         [args.cohort + etype + '.loadfile.txt' for etype in 
                          ('.Participants', '.Samples', '.SampleSet')]))
    
    for loadfile in loadfiles.values():
        if not os.path.isfile(loadfile):
            logging.error("Loadfile not found: %s. Exiting.", loadfile)
            sys.exit(1)
    
    fromproject, fromspace = args.methods.split('/')
    if args.workspace is None:
        args.workspace = 'awg_' + args.cohort
    
    args.workspace = create_workspace(args.project, args.workspace)
    
    # Add ACLs
    set_acl('owner', args)
    set_acl('reader', args)
    set_acl('writer', args)
    
    # Set args.workspace annotations
    logging.info('Setting package to "true"')
    fissfc('-y', 'attr_set', '-p', args.project, '-w', args.workspace, '-a', 'package',
           '-v', 'true')
    
    # Copy method configs
    logging.info('Copying method configs from %s to %s/%s', args.methods,
                 args.project, args.workspace)
    
    for config in get_configs(args.workflow):
        fissfc('config_copy', '-c', config, '-n', args.namespace,
               '-p', fromproject, '-s', fromspace, '-P', args.project,
               '-S', args.workspace)
    
    # Load Entities
    logging.info('Loading Participants')
    fissfc('entity_import', '-p', args.project, '-w', args.workspace,
           '-f', loadfiles['participants'])
    
    logging.info('Loading Samples')
    fissfc('entity_import', '-p', args.project, '-w', args.workspace,
           '-f', loadfiles['samples'])
    
    logging.info('Loading Sample Sets')
    fissfc('entity_import', '-p', args.project, '-w', args.workspace,
           '-f', loadfiles['sample_sets'])
    
    # Initiate supervisor mode
    recover = args.workspace + '.json'
    logging.info('Initiating run. Recovery file is at:\n\t' + recover +
                 '\nIf run fails due to FireCloud issues, it can be ' +
                 'continued by running:\n\t' +
                 'fissfc supervise_recover ' + recover)
    if args.dashboard:
        logging.info('Initiating dashboard generation cron job')
        try:
            cron_cmd = ['gdac_cron', 'awg', 'add', '-c',
                        'gdac_dashboard ' + args.workspace]
            subprocess.check_output(cron_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as cpe:
            cron_cmd[-1] = '"{}"'.format(cron_cmd[-1])
            if cpe.output:
                logging.warning(cpe.output)
            logging.warning('Failed to add Dashboard cron job. Please run \n' +
                            '\t{}\n'.format(' '.join(cron_cmd)) +
                            'from a CGA server')
    
    fissfc('-y', 'supervise', '-p', args.project, '-w', args.workspace,
           '-n', args.namespace, '-j', recover, args.workflow)

if __name__ == '__main__':
    main()
