#!/usr/bin/env python

import argparse

import logger


def get_base_argument_parsers(additional_step_options=None):
    parsers = {'parser': argparse.ArgumentParser()}
    parsers['parser'].add_argument('--loglevel', choices=['INFO', 'WARNING', 'ERROR'], default='INFO')
    parsers['parser'].add_argument('--logfile', action='append', nargs=2, metavar=('LOGFILE', '{INFO,WARNING,ERROR}'))

    parsers['command'] = parsers['parser'].add_subparsers(dest='command')
    parsers['command'].required = True
    parsers['reduce'] = argparse.ArgumentParser(add_help=False)
    parsers['reduce'].add_argument('--trace', action='store_true', help='Only simulate DRS commands, requires pp files')
    parsers['reduce'].add_argument('--runid', nargs='+', help='Only process observations belonging to the runid(s)')
    parsers['reduce'].add_argument('--target', nargs='+', help='Only process observations of the target(s)')

    parsers['steps'] = ['preprocess', 'ppcal', 'ppobj',
                        'calibrations', 'badpix', 'loc', 'shape', 'flat', 'thermal', 'wave',
                        'objects', 'snronly', 'extract', 'leak', 'fittellu', 'ccf', 'pol', 'products']
    if additional_step_options:
        parsers['steps'].extend(additional_step_options)
    parsers['reduce'].add_argument('--steps', nargs='+', choices=parsers['steps'])

    parsers['multinight'] = argparse.ArgumentParser(parents=[parsers['reduce']], add_help=False)
    parsers['multinight'].add_argument('--parallel', type=int, help='If used, number of parallel processes to run')
    parsers['command'].add_parser('all', parents=[parsers['multinight']], help='Reduce all nights')
    parsers['qrunid'] = parsers['command'].add_parser('qrunid', parents=[parsers['multinight']],
                                                      help='Reduce all nights belonging to qrunid')
    parsers['qrunid'].add_argument('qrunid')

    parsers['night'] = argparse.ArgumentParser(parents=[parsers['reduce']], add_help=False)
    parsers['night'].add_argument('night')
    parsers['command'].add_parser('night', parents=[parsers['night']], help='Reduce a night directory')
    parsers['subset'] = parsers['command'].add_parser('subset', parents=[parsers['night']], help='Reduce part of night')
    parsers['subset args'] = parsers['subset'].add_mutually_exclusive_group(required=True)
    parsers['subset args'].add_argument('--range', nargs=2, help='Files at start and end of range')
    parsers['subset args'].add_argument('--list', nargs='+', help='Specific list of files')
    parsers['file'] = parsers['command'].add_parser('file', parents=[parsers['night']], help='Reduce a single file')
    parsers['file'].add_argument('filename')
    parsers['sequence'] = parsers['command'].add_parser('sequence', parents=[parsers['night']],
                                                        help='Reduce a sequence of files together')
    parsers['sequence'].add_argument('filenames', nargs='+')
    return parsers


def reduce_execute(args, drs_class, steps_class, filters_class):
    if args.steps:
        steps = steps_class.from_keys(args.steps)
    else:
        steps = steps_class.all()
    trigger = drs_class(steps, trace=args.trace)
    filters = filters_class(runids=args.runid, targets=args.target)
    if args.command == 'all':
        trigger.reduce_all_nights(filters=filters, num_processes=args.parallel)
    elif args.command == 'qrunid':
        trigger.reduce_qrun(args.qrunid, filters=filters, num_processes=args.parallel)
    elif args.command == 'night':
        trigger.reduce_night(args.night, filters=filters)
    elif args.command == 'subset':
        if args.list:
            trigger.reduce([trigger.exposure(args.night, filename) for filename in args.list])
        elif args.range:
            trigger.reduce_range(args.night, args.range[0], args.range[1], filters=filters)
    elif args.command == 'file':
        exposure = trigger.exposure(args.night, args.filename)
        if trigger.preprocess(exposure):
            trigger.process_file(exposure)
    elif args.command == 'sequence':
        trigger.process_sequence([trigger.exposure(args.night, filename) for filename in args.filenames])


if __name__ == '__main__':
    from trigger.common import DrsSteps
    from trigger.drstrigger import DrsTrigger
    from trigger.fileselector import FileSelectionFilters

    parsers = get_base_argument_parsers()
    parsers['command'].add_parser('version', help='DRS version information')

    args = parsers['parser'].parse_args()

    logger.configure_logger(console_level=args.loglevel, log_files=args.logfile)

    if args.command == 'version':
        print(DrsTrigger.drs_version())
    else:
        reduce_execute(args, DrsTrigger, DrsSteps, FileSelectionFilters)
