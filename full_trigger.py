#!/usr/bin/env python

import datetime

from drsloader import DrsLoader
from logger import configure_logger
from offline_trigger import get_base_argument_parsers, reduce_execute
from realtime import load_and_start_realtime, run_listener

if __name__ == '__main__':
    parsers = get_base_argument_parsers(additional_step_options=['distribute', 'database', 'distraw', 'distql'])
    parsers['parser'].add_argument('--config', help='Use custom DRS config directory')
    version_parse = parsers['command'].add_parser('version', help='DRS version information')
    version_flags = version_parse.add_mutually_exclusive_group(required=False)
    version_flags.add_argument('--drs', action='store_true')
    version_flags.add_argument('--trigger', action='store_true')
    realtime_parse = parsers['command'].add_parser('realtime', help='Reduce files from observing session and update DB')
    realtime_parse.add_argument('--port', type=int, default=9998)
    realtime_parse.add_argument('--processes', type=int, default=4)
    realtime_parse.add_argument('--steps', nargs='+', choices=parsers['steps'])
    realtime_parse.add_argument('--trace', action='store_true', help='Only simulate DRS commands, requires pp files')

    args = parsers['parser'].parse_args()

    log_files = args.logfile if args.logfile else []
    if args.command == 'realtime':
        timestamp = datetime.datetime.now().strftime("error-report-%Y%m%d-%H%M%S")
        log_files.append((timestamp, 'ERROR'))
    configure_logger(console_level=args.loglevel, log_files=log_files)

    if args.command == 'realtime':
        queue = run_listener(args.port)
        load_and_start_realtime(args.processes, queue, args.config, args.steps, args.trace)
    else:
        loader = DrsLoader(args.config)
        cfht = loader.get_loaded_trigger_module()
        from cfht import CfhtDrsTrigger, CfhtDrsSteps, FileSelectionFilters

        if args.command == 'version':
            if args.drs:
                print(CfhtDrsTrigger.drs_version())
            elif args.trigger:
                print(CfhtDrsTrigger.trigger_version())
            else:
                print('DRS version', CfhtDrsTrigger.drs_version(), '-',
                      'Trigger version', CfhtDrsTrigger.trigger_version())
        else:
            reduce_execute(args, CfhtDrsTrigger, CfhtDrsSteps, FileSelectionFilters)
