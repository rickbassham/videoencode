#!/usr/bin/python

# Need to add error and reason columns

from manager import Packet, Manager
from threading import Event, Lock
import json
import time
import datetime

import signal
import sys
e = Event()
def signal_handler(signal, frame):
    global e
    print('You pressed Ctrl+C!')
    e.set()
signal.signal(signal.SIGINT, signal_handler)


import sqlite3
class DataManager(Manager):
    def __init__(self):
        Manager.__init__(self, 'DataManager')

    def update_schema(self):
        self.cursor.execute('create table if not exists Version (Version INTEGER)')
        self.conn.commit()

        version = self.current_version()

        schema_changes = [
            'create table encodingqueue (CreatedTimestamp INTEGER, LastUpdatedTimestamp INTEGER, Profile TEXT, Priority INTEGER, Status TEXT, ShouldStop INTEGER, PercentComplete REAL, FrameRate REAL, InputPath TEXT, OutputPath TEXT)',
        ]

        if version < len(schema_changes):
            for stmt in schema_changes[version:]:
                self.cursor.execute(stmt)
                if version == 0:
                    self.cursor.execute('INSERT INTO Version (Version) VALUES (?)', (version + 1,))
                else:
                    self.cursor.execute('UPDATE Version SET Version = ?', (version + 1,))
                self.conn.commit()

    def current_version(self):
        self.cursor.execute('SELECT Version FROM Version')
        version = self.cursor.fetchone()

        if version is None:
            version = 0
        else:
            version = version[0]

        return version

    def add_encoding_queue_item(self, obj):
        self.cursor.execute((
            "insert into encodingqueue "
            "(CreatedTimestamp, LastUpdatedTimestamp, Profile, Priority, Status,"
            " ShouldStop, PercentComplete, FrameRate, InputPath,"
            " OutputPath) VALUES "
            "(:CreatedTimestamp, :LastUpdatedTimestamp, :Profile, :Priority, :Status,"
            " :ShouldStop, :PercentComplete, :FrameRate, :InputPath,"
            " :OutputPath)"), obj)
        self.conn.commit()

    def update_encode(self, obj):
        obj['LastUpdatedTimestamp'] = int(time.time())

        self.cursor.execute((
            'UPDATE encodingqueue SET '
            '   LastUpdatedTimestamp = :LastUpdatedTimestamp,'
            '   Status = :Status,'
            '   PercentComplete = :PercentComplete,'
            '   FrameRate = :FrameRate '
            'WHERE'
            '   RowID = :RowID'
            ), obj)
        self.conn.commit()

    def get_next(self, profiles):
        encoding_list = self.encodingqueue_list(profiles, pending_only=True, limit=1)

        if len(encoding_list) > 0:
            self.update_encode({
                'Status': 'Starting',
                'PercentComplete': 0.0,
                'FrameRate': 0.0,
                'RowID': encoding_list[0]['RowID']
                })

        return encoding_list

    def encodingqueue_list(self, profiles=[], pending_only=False, limit=None):
        query = (
            'SELECT'
            '   RowID,'
            '   CreatedTimestamp,'
            '   LastUpdatedTimestamp,'
            '   Profile,'
            '   Priority,'
            '   Status,'
            '   ShouldStop,'
            '   PercentComplete,'
            '   FrameRate,'
            '   InputPath,'
            '   OutputPath '
            'FROM'
            '   encodingqueue ')

        parameters = []

        if len(profiles) > 0:
            query = query + 'WHERE '

            if len(profiles) > 1:
                query = query + ' Profile in (%s) ' % ','.join('?'*len(profiles))
                parameters = list(profiles)
            else:
                query = query + ' Profile = ?'
                parameters = [profiles[0]]

            if pending_only:
                query = query + " AND Status = 'Pending' "
        elif pending_only:
            query = query + "WHERE Status = 'Pending' "

        query = query + ' ORDER BY Priority ASC, CreatedTimestamp ASC '

        if limit is not None:
            query = query + ' LIMIT ? '
            parameters.append(limit)

        print query, parameters

        if parameters is not None:
            self.cursor.execute(query, parameters)
        else:
            self.cursor.execute(query)

        rows = self.cursor.fetchall()

        encoding_list = []

        for row in rows:
            encoding_list.append({
                'RowID': row[0],
                'CreatedTimestamp': row[1],
                'LastUpdatedTimestamp': row[2],
                'Profile': row[3],
                'Priority': row[4],
                'Status': row[5],
                'ShouldStop': row[6],
                'PercentComplete': row[7],
                'FrameRate': row[8],
                'InputPath': row[9],
                'OutputPath': row[10],
                })

        return encoding_list

    def get_active(self):
        query = (
            "SELECT"
            "   RowID,"
            "   CreatedTimestamp,"
            "   LastUpdatedTimestamp,"
            "   Profile,"
            "   Priority,"
            "   Status,"
            "   ShouldStop,"
            "   PercentComplete,"
            "   FrameRate,"
            "   InputPath,"
            "   OutputPath "
            "FROM"
            "   encodingqueue "
            "WHERE"
            "   Status not in ('Complete', 'Pending', 'Skipped', 'Error', 'FileNotFound', 'InvalidInputFile') "
        )

        print query

        self.cursor.execute(query)

        rows = self.cursor.fetchall()

        encoding_list = []

        for row in rows:
            encoding_list.append({
                'RowID': row[0],
                'CreatedTimestamp': row[1],
                'LastUpdatedTimestamp': row[2],
                'Profile': row[3],
                'Priority': row[4],
                'Status': row[5],
                'ShouldStop': row[6],
                'PercentComplete': row[7],
                'FrameRate': row[8],
                'InputPath': row[9],
                'OutputPath': row[10],
                })

        return encoding_list

    def reset_to_pending(self, statuses=[]):
        query = (
            "UPDATE encodingqueue"
            "   Set Status = 'Pending' "
            "WHERE "
        )

        parameters = []

        if len(statuses) > 0:
            if len(statuses) > 1:
                query = query + ' Status in (%s) ' % ','.join('?'*len(statuses))
                parameters = list(statuses)
            else:
                query = query + ' Status = ?'
                parameters = [statuses[0]]
        else:
            raise Exception("No statuses specified.")

        print query, parameters

        if parameters is not None:
            self.cursor.execute(query, parameters)
        else:
            self.cursor.execute(query)

        count = self.cursor.rowcount

        self.conn.commit()

        return count

    def get_all_with_status(self, statuses=[]):
        query = (
            "SELECT"
            "   RowID,"
            "   CreatedTimestamp,"
            "   LastUpdatedTimestamp,"
            "   Profile,"
            "   Priority,"
            "   Status,"
            "   ShouldStop,"
            "   PercentComplete,"
            "   FrameRate,"
            "   InputPath,"
            "   OutputPath "
            "FROM"
            "   encodingqueue "
            "WHERE"
        )

        parameters = []

        if len(statuses) > 0:
            if len(statuses) > 1:
                query = query + ' Status in (%s) ' % ','.join('?'*len(statuses))
                parameters = list(statuses)
            else:
                query = query + ' Status = ?'
                parameters = [statuses[0]]
        else:
            raise Exception("No statuses specified.")

        print query, parameters

        self.cursor.execute(query, parameters)

        rows = self.cursor.fetchall()

        encoding_list = []

        for row in rows:
            encoding_list.append({
                'RowID': row[0],
                'CreatedTimestamp': row[1],
                'LastUpdatedTimestamp': row[2],
                'Profile': row[3],
                'Priority': row[4],
                'Status': row[5],
                'ShouldStop': row[6],
                'PercentComplete': row[7],
                'FrameRate': row[8],
                'InputPath': row[9],
                'OutputPath': row[10],
                })

        return encoding_list

    def starting(self):
        self.conn = sqlite3.connect('encodingqueue.db')
        self.cursor = self.conn.cursor()
        self.update_schema()

    def stopping(self):
        self.conn.close()

    def process(self, packet):
        if packet.key == "index":
            self.process_request_for_index(packet)
        elif packet.key == "version":
            self.process_request_for_version(packet)
        elif packet.key == "add_encode":
            self.process_add_encode(packet)
        elif packet.key == "encode_list":
            self.process_request_for_encode_list(packet)
        elif packet.key == "get_next":
            self.process_request_for_get_next(packet)
        elif packet.key == "update_encode":
            self.process_update_encode(packet)
        elif packet.key == "reset_to_pending":
            self.process_reset_to_pending(packet)
        elif packet.key == "get_active":
            self.process_request_for_active(packet)
        elif packet.key == "get_all_with_status":
            self.process_request_all_with_status(packet)

    def process_request_for_index(self, packet):
        packet.payload['list'] = self.encodingqueue_list()
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_encode_list(self, packet):
        packet.payload['list'] = self.encodingqueue_list(packet.payload['profiles'])
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_get_next(self, packet):
        packet.payload['list'] = self.get_next(packet.payload['profiles'])
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_version(self, packet):
        packet.payload['version'] = self.current_version()
        packet.return_to_sender()
        self.send(packet)

    def process_add_encode(self, packet):
        self.add_encoding_queue_item(packet.payload['obj'])
        packet.return_to_sender()
        self.send(packet)

    def process_update_encode(self, packet):
        self.update_encode(packet.payload['obj'])
        packet.return_to_sender()
        self.send(packet)

    def process_reset_to_pending(self, packet):
        packet.payload['count'] = self.reset_to_pending(packet.payload['statuses'])
        packet.return_to_sender()
        self.send(packet)

    def process_request_all_with_status(self, packet):
        packet.payload['list'] = self.get_all_with_status(packet.payload['statuses'])
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_active(self, packet):
        packet.payload['list'] = self.get_active()
        packet.return_to_sender()
        self.send(packet)


import cherrypy
class RequestManager(Manager):
    def __init__(self):
        Manager.__init__(self, 'RequestManager')
        cherrypy.server.socket_host = '0.0.0.0'
        cherrypy.config.update({'engine.autoreload.on': False})
        cherrypy.tree.mount(self, '/')
        cherrypy.engine.start()
        self._next_request_id = 0
        self._requests = {}
        self._requests_lock = Lock()

    def process(self, packet):
        request_id = packet.payload.get('request_id', None)

        if request_id is not None:
            self._requests[request_id]['packet'] = packet
            self._requests[request_id]['event'].set()

    def stop(self):
        cherrypy.engine.exit()
        Manager.stop(self)

    def register_request(self):
        request_id = None

        with self._requests_lock:
            request_id = self._next_request_id
            self._next_request_id = self._next_request_id + 1
            self._requests[request_id] = { 'event': Event(), 'packet': None }

        return request_id

    def wait_for_response(self, request_id, timeout=10):
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

        return response_packet.payload

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

        return response_packet.payload

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_next(self, profile=None):
        request_id = self.register_request()

        if profile is None:
            profile = []

        if isinstance(profile, basestring):
            profile = [profile]

        self.send(Packet('get_next', 'DataManager', self.name, payload={ 'request_id': request_id, 'profiles': profile }))

        response_packet = self.wait_for_response(request_id)

        return response_packet.payload

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def version(self):
        request_id = self.register_request()

        self.send(Packet('version', 'DataManager', self.name, payload={ 'request_id': request_id }))

        response_packet = self.wait_for_response(request_id)

        return response_packet.payload

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def add_encode(self):
        request_id = self.register_request()

        obj = cherrypy.request.json

        self.send(Packet('add_encode', 'DataManager', self.name, payload={ 'request_id': request_id, 'obj': obj }))
        response_packet = self.wait_for_response(request_id)

        return json.dumps(response_packet.payload)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def update_encode(self):
        request_id = self.register_request()

        obj = cherrypy.request.json

        self.send(Packet('update_encode', 'DataManager', self.name, payload={ 'request_id': request_id, 'obj': obj }))
        response_packet = self.wait_for_response(request_id)

        return json.dumps(response_packet.payload)

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

        return response_packet.payload

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

        return response_packet.payload

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_active(self):
        request_id = self.register_request()

        self.send(Packet('get_active', 'DataManager', self.name, payload={ 'request_id': request_id }))

        response_packet = self.wait_for_response(request_id)

        return response_packet.payload


def Application():
    global e

    mgr1 = DataManager()
    mgr2 = RequestManager()

    mgr1.connect_to(mgr2)
    mgr2.connect_to(mgr1)

    mgr1.start()
    mgr2.start()

    print 'In main run loops...'

    while not e.wait(1):
        pass

    print 'Stopping managers...'

    mgr1.stop()
    mgr2.stop()


if __name__ == '__main__':
    Application()
