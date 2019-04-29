from multiprocessing import Pool, Queue

import cherrypy
from flask import Flask, request

from logger import logger
from realtime import realtime


def run_listener(port, num_processes):
    app = Flask('realtime-server')
    file_queue = Queue()

    @app.route('/_status', methods=['GET'])
    def status_check():
        return '{"success": true}', 200

    @app.route('/trigger', methods=['POST'])
    def realtime_trigger():
        filename = request.args.get('filename')
        try:
            file_queue.put(filename)
            return '{"success": true}', 200
        except Exception as error:
            return '{"success": false}', 500

    app.base_url = 'http://localhost:' + port
    cherrypy.tree.graft(app.wsgi_app, '/')
    cherrypy.config.update({'server.socket_host': '0.0.0.0',
                            'server.socket_port': int(port),
                            'engine.autoreload.on': False,
                            })
    logger.info('starting server')
    cherrypy.engine.start()
    logger.info('waiting for requests')
    Pool(num_processes, worker, (file_queue,))


def worker(queue):
    while True:
        item = queue.get(True)
        logger.info('Processing %s', item)
        try:
            realtime(item)
        except:
            logger.info('An error occurred while processing %s', item)
