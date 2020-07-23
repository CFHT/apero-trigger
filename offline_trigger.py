#!/data/spirou/offline/venv/bin/python3 -u

import argparse

import logger


def get_base_argument_parser(additional_step_options = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel', choices=['INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--logfile', action='append', nargs=2, metavar=('LOGFILE', '{INFO,WARNING,ERROR}'))

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True
    reduce_parser = argparse.ArgumentParser(add_help=False)
    reduce_parser.add_argument('--trace', action='store_true', help='Only simulate DRS commands, requires pp files')
    # TODO load default ccf parameters from DRS
    reduce_parser.add_argument('--ccfmask', type=str, default='masque_sept18_andres_trans50.mas')
    reduce_parser.add_argument('--ccfv0', type=float, default=0)
    reduce_parser.add_argument('--ccfrange', type=float, default=300)
    reduce_parser.add_argument('--ccfstep', type=float, default=0.5)
    reduce_parser.add_argument('--runid', nargs='+', help='Only process files belonging to the runid(s)')
    reduce_parser.add_argument('--target', nargs='+', help='Only process files that are observations of the target(s)')

    steps_options = ['preprocess', 'ppcal', 'ppobj',
                     'calibrations', 'badpix', 'loc', 'shape', 'flat', 'thermal', 'wave',
                     'objects', 'extract', 'leak', 'fittellu', 'ccf', 'pol', 'products']
    if additional_step_options:
        steps_options.extend(additional_step_options)
    reduce_parser.add_argument('--steps', nargs='+', choices=steps_options)

    multi_night_parser = argparse.ArgumentParser(parents=[reduce_parser], add_help=False)
    multi_night_parser.add_argument('--parallel', type=int, help='If used, number of parallel processes to run')
    subparsers.add_parser('all', parents=[multi_night_parser], help='Reduce all nights')
    reduce_qrun_parser = subparsers.add_parser('qrunid', parents=[multi_night_parser],
                                               help='Reduce all nights belonging to qrunid')
    reduce_qrun_parser.add_argument('qrunid')

    single_night_parser = argparse.ArgumentParser(parents=[reduce_parser], add_help=False)
    single_night_parser.add_argument('night')
    subparsers.add_parser('night', parents=[single_night_parser], help='Reduce a night directory')
    reduce_subset_parser = subparsers.add_parser('subset', parents=[single_night_parser], help='Reduce part of night')
    reduce_subset_flags = reduce_subset_parser.add_mutually_exclusive_group(required=True)
    reduce_subset_flags.add_argument('--range', nargs=2, help='Files at start and end of range')
    reduce_subset_flags.add_argument('--list', nargs='+', help='Specific list of files')
    reduce_file_parse = subparsers.add_parser('file', parents=[single_night_parser], help='Reduce a single file')
    reduce_file_parse.add_argument('filename')
    reduce_sequence_parse = subparsers.add_parser('sequence', parents=[single_night_parser],
                                                  help='Reduce a sequence of files together')
    reduce_sequence_parse.add_argument('filenames', nargs='+')
    return parser, subparsers


def reduce_execute(args, drs_class, steps_class, filters_class, ccf_params_class):
    ccf_params = ccf_params_class(args.ccfmask, args.ccfv0, args.ccfrange, args.ccfstep)
    if args.steps:
        steps = steps_class.from_keys(args.steps)
    else:
        steps = steps_class.all()
    trigger = drs_class(steps, ccf_params=ccf_params, trace=args.trace)
    filters = filters_class(runids=args.runid, targets=args.target)
    if args.command == 'all':
        trigger.reduce_all_nights(filters=filters, num_processes=args.parallel)
    elif args.command == 'qrunid':
        trigger.reduce_qrun(args.qrunid, filters=filters, num_processes=args.parallel)
    elif args.command == 'night':
        trigger.reduce_night(args.night, filters=filters)
    elif args.command == 'subset':
        if args.list:
            trigger.reduce([trigger.Exposure(args.night, filename) for filename in args.list])
        elif args.range:
            trigger.reduce_range(args.night, args.range[0], args.range[1], filters=filters)
    elif args.command == 'file':
        exposure = trigger.Exposure(args.night, args.filename)
        if trigger.preprocess(exposure):
            trigger.process_file(exposure)
    elif args.command == 'sequence':
        trigger.process_sequence([trigger.Exposure(args.night, filename) for filename in args.filenames])


if __name__ == '__main__':
    from trigger.common import DrsSteps, CcfParams
    from trigger.drstrigger import DrsTrigger
    from trigger.fileselector import FileSelectionFilters

    parser, subparsers = get_base_argument_parser()
    subparsers.add_parser('version', help='DRS version information')

    args = parser.parse_args()

    logger.configure_logger(console_level=args.loglevel, log_files=args.logfile)

    if args.command == 'version':
        print(DrsTrigger.drs_version())
    else:
        reduce_execute(args, DrsTrigger, DrsSteps, FileSelectionFilters, CcfParams)
