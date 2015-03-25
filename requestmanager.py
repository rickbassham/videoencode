#!/usr/bin/python

from manager import Packet, Manager

from threading import Event, Lock

import cherrypy
import datetime

class RequestManager(Manager):
    def __init__(self):
        Manager.__init__(self, 'RequestManager')

    def process(self, packet):
        request_id = packet.payload.get('request_id', None)

        if request_id is not None:
            self._requests[request_id]['packet'] = packet
            self._requests[request_id]['event'].set()

    def starting(self):
        cherrypy.server.socket_host = '0.0.0.0'
        cherrypy.config.update({'engine.autoreload.on': False})
        cherrypy.tree.mount(self, '/')
        cherrypy.engine.start()
        self._next_request_id = 0
        self._requests = {}
        self._requests_lock = Lock()

    def stopping(self):
        cherrypy.engine.exit()

    def register_request(self):
        request_id = None

        with self._requests_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
            self._requests[request_id] = { 'event': Event(), 'packet': None }

        return request_id

    def wait_for_response(self, request_id, timeout=120):
        req = None

        with self._requests_lock:
            req = self._requests[request_id]

        e = req['event']

        start = datetime.datetime.now()

        while not e.wait(0.1):
            if (datetime.datetime.now() - start).total_seconds() > timeout:
                e.set()

        response_packet = None

        with self._requests_lock:
            response_packet = self._requests[request_id]['packet']
            del self._requests[request_id]

        return response_packet

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        request_id = self.register_request()

        self.send(Packet('index', 'DataManager', self.name, payload={ 'request_id': request_id }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            l = payloadresponse_packet.payload.get('list', None)
            if l is not None:
                response_packet.payload['count'] = len(l)

            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def encode_list(self, profile=None):
        request_id = self.register_request()

        if profile is None:
            profile = []

        if isinstance(profile, basestring):
            profile = [profile]

        self.send(Packet('encode_list', 'DataManager', self.name, payload={ 'request_id': request_id, 'profiles': profile }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            l = response_packet.payload.get('list', None)
            if l is not None:
                response_packet.payload['count'] = len(l)

            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_next(self, profile=None, status=None):
        request_id = self.register_request()

        if profile is None:
            profile = []

        if isinstance(profile, basestring):
            profile = [profile]

        if status is None:
            status = []

        if isinstance(status, basestring):
            status = [status]

        self.send(Packet('get_next', 'DataManager', self.name, payload={ 'request_id': request_id, 'profiles': profile, 'statuses': status }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def version(self):
        request_id = self.register_request()

        self.send(Packet('version', 'DataManager', self.name, payload={ 'request_id': request_id }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def add_encode(self):
        request_id = self.register_request()

        obj = cherrypy.request.json

        self.send(Packet('add_encode', 'DataManager', self.name, payload={ 'request_id': request_id, 'obj': obj }))
        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def update_encode(self):
        request_id = self.register_request()

        obj = cherrypy.request.json

        self.send(Packet('update_encode', 'DataManager', self.name, payload={ 'request_id': request_id, 'obj': obj }))
        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def reset_to_pending(self, status=None):
        request_id = self.register_request()

        if status is None:
            status = []

        if isinstance(status, basestring):
            status = [status]

        self.send(Packet('reset_to_pending', 'DataManager', self.name, payload={ 'request_id': request_id, 'statuses': status }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_all_with_status(self, status=None):
        request_id = self.register_request()

        if status is None:
            status = []

        if isinstance(status, basestring):
            status = [status]

        self.send(Packet('get_all_with_status', 'DataManager', self.name, payload={ 'request_id': request_id, 'statuses': status }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            l = response_packet.payload.get('list', None)
            if l is not None:
                response_packet.payload['count'] = len(l)

            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_active(self):
        request_id = self.register_request()

        self.send(Packet('get_active', 'DataManager', self.name, payload={ 'request_id': request_id }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            l = response_packet.payload.get('list', None)
            if l is not None:
                response_packet.payload['count'] = len(l)

            return response_packet.payload
        else:
            return None

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_count_per_status(self):
        request_id = self.register_request()

        self.send(Packet('get_count_per_status', 'DataManager', self.name, payload={ 'request_id': request_id }))

        response_packet = self.wait_for_response(request_id)

        if response_packet is not None:
            return response_packet.payload
        else:
            return None
