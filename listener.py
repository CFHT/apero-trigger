import cherrypy
from flask import Flask, request
from realtime import realtime


def run_listener(port):
    app = Flask("realtime-server")

    @app.route("/trigger", methods=["POST"])
    def realtime_trigger():
        filename = request.args.get('filename')
        realtime(filename)
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
