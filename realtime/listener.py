from __future__ import annotations

from multiprocessing import Queue
from pathlib import Path

import cherrypy
from flask import Flask, request

from logger import log


def run_listener(port: int) -> Queue[Path]:
    app = Flask('realtime-server')
    file_queue = Queue()

    @app.route('/_status', methods=['GET'])
    def status_check():
        return '{"success": true}', 200

    @app.route('/trigger', methods=['POST'])
    def realtime_trigger():
        filename = request.args.get('filename')
        try:
            file_queue.put(Path(filename))
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
