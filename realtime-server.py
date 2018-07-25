#!/data/spirou/venv/bin/python3

import cherrypy, argparse
from flask import Flask, request
import realtime

def main(port):
    app = Flask("realtime-server")

    @app.route("/trigger", methods=["POST"])
    def realtime_trigger():
        filename = request.args.get('filename')
        realtime.main(filename)
        return '{"success": true}', 200

    app.base_url = "http://localhost:" + port
    cherrypy.tree.graft(app.wsgi_app, '/')
    cherrypy.config.update({'server.socket_host': '0.0.0.0',
                            'server.socket_port': int(port),
                            'engine.autoreload.on': False,
                            })
    print("starting server")
    cherrypy.engine.start()
    print("waiting for requests")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('port')
    args = parser.parse_args()
    try:
        main(args.port)
    except Exception as e:
        print(e)
