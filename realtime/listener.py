from __future__ import annotations

from multiprocessing import Queue, Process
from pathlib import Path

import cherrypy
from flask import Flask, request

from logger import log
from trigger import CfhtTrigger


def run_listener(port: int) -> Queue[Path]:
    app = Flask('realtime-server')
    file_queue = Queue()

    @app.route('/_status', methods=['GET'])
    def status_check():
        return '{"success": true}', 200

    @app.route('/file', methods=['POST'])
    def realtime_trigger():
        filename = request.args.get('filename')
        try:
            file_queue.put(Path(filename))
            return '{"success": true}', 200
        except Exception:
            return '{"success": false}', 500

    @app.route('/calibrations', methods=['POST'])
    def process_calibrations():
        night = request.args.get('night')
        try:
            trigger = CfhtTrigger()
            p = Process(target=trigger.process_calibrations, args=(night,))
            p.start()
            return '{"success": true}', 200
        except Exception:
            return '{"success": false}', 500

    app.base_url = 'http://localhost:' + str(port)
    cherrypy.tree.graft(app.wsgi_app, '/')
    cherrypy.config.update({'server.socket_host': '0.0.0.0',
                            'server.socket_port': int(port),
                            'engine.autoreload.on': False,
                            })
    log.info('starting server')
    cherrypy.engine.start()
    log.info('waiting for requests')
    return file_queue
