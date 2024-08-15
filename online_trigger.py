#!/usr/bin/env python
import argparse
import datetime

from logger import configure_logger
from realtime import load_and_start_realtime, run_listener
from trigger import CfhtTrigger

if __name__ == '__main__':
    parsers = {'parser': argparse.ArgumentParser()}
    parsers['parser'].add_argument('--loglevel', choices=['INFO', 'WARNING', 'ERROR'], default='INFO')
    parsers['parser'].add_argument('--logfile', action='append', nargs=2, metavar=('LOGFILE', '{INFO,WARNING,ERROR}'))

    parsers['command'] = parsers['parser'].add_subparsers(dest='command')
    parsers['command'].required = True

    version_parse = parsers['command'].add_parser('version', help='DRS version information')
    version_flags = version_parse.add_mutually_exclusive_group(required=False)
    version_flags.add_argument('--drs', action='store_true')
    version_flags.add_argument('--trigger', action='store_true')
    realtime_parse = parsers['command'].add_parser('realtime', help='Reduce files from observing session and update DB')
    realtime_parse.add_argument('--port', type=int, default=9998)
    realtime_parse.add_argument('--processes', type=int, default=4)
    realtime_parse.add_argument('--trace', action='store_true', help='Only simulate DRS commands, requires pp files')

    args = parsers['parser'].parse_args()

    log_files = args.logfile if args.logfile else []
    if args.command == 'realtime':
        timestamp = datetime.datetime.now().strftime("error-report-%Y%m%d-%H%M%S")
        log_files.append((timestamp, 'ERROR'))
    configure_logger(console_level=args.loglevel, log_files=log_files)

    if args.command == 'realtime':
        queue = run_listener(args.port)
        load_and_start_realtime(args.processes, queue, args.trace)
    elif args.command == 'version':
        if args.drs:
            print(CfhtTrigger.drs_version())
        elif args.trigger:
            print(CfhtTrigger.trigger_version())
        else:
            print('DRS version', CfhtTrigger.drs_version(), '-',
                  'Trigger version', CfhtTrigger.trigger_version())
