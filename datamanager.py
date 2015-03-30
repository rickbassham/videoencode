#!/usr/bin/python

from manager import Packet, Manager

import time
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
            'alter table encodingqueue add column EncodingReasons TEXT',
            'alter table encodingqueue add column ErrorText TEXT',
            'alter table encodingqueue add column EncodingTime INTEGER',

        ]

        if version < len(schema_changes):
            for stmt in schema_changes[version:]:
                self.cursor.execute(stmt)
                if version == 0:
                    self.cursor.execute('INSERT INTO Version (Version) VALUES (?)', (version + 1,))
                else:
                    self.cursor.execute('UPDATE Version SET Version = ?', (version + 1,))
                self.conn.commit()
                version += 1

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

        print 'update_object', obj

        self.cursor.execute((
            'UPDATE encodingqueue SET '
            '   EncodingReasons = :EncodingReasons,'
            '   ErrorText = :ErrorText,'
            '   EncodingTime = :EncodingTime,'
            '   LastUpdatedTimestamp = :LastUpdatedTimestamp,'
            '   Status = :Status,'
            '   PercentComplete = :PercentComplete,'
            '   FrameRate = :FrameRate '
            'WHERE'
            '   RowID = :RowID'
            ), obj)
        self.conn.commit()

    def get_next(self, profiles, statuses):
        encoding_list = self.encodingqueue_list(profiles, statuses, limit=1)

        if len(encoding_list) > 0:
            self.update_encode({
                'Status': 'Starting',
                'PercentComplete': 0.0,
                'FrameRate': 0.0,
                'RowID': encoding_list[0]['RowID'],
                'EncodingReasons': None,
                'ErrorText': None,
                'EncodingTime': 0
            })

        return encoding_list

    def encodingqueue_list(self, profiles=[], statuses=[], limit=None):
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
            '   OutputPath,'
            '   EncodingReasons,'
            '   ErrorText,'
            '   EncodingTime '
            'FROM'
            '   encodingqueue ')

        parameters = []

        where_added = False

        if len(profiles) > 0:
            query = query + 'WHERE '

            if len(profiles) > 1:
                query = query + ' Profile in (%s) ' % ','.join('?'*len(profiles))
                parameters.extend(profiles)
            else:
                query = query + ' Profile = ?'
                parameters.extend([profiles[0]])

            where_added = True

        if len(statuses) > 0:
            if where_added:
                query = query + ' AND '
            else:
                query = query + ' WHERE '

            if len(statuses) > 1:
                query = query + ' Status in (%s) ' % ','.join('?'*len(statuses))
                parameters.extend(statuses)
            else:
                query = query + ' Status = ?'
                parameters.extend([statuses[0]])

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
                'EncodingReasons': row[11],
                'ErrorText': row[12],
                'EncodingTime': row[13]
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
            "   OutputPath,"
            '   EncodingReasons,'
            '   ErrorText,'
            '   EncodingTime '
            "FROM"
            "   encodingqueue "
            "WHERE"
            "   Status not in ('Complete', 'Pending', 'PendingFull', 'Skipped', 'Error', 'Exception', 'FileNotFound', 'InvalidInputFile') "
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
                'EncodingReasons': row[11],
                'ErrorText': row[12],
                'EncodingTime': row[13]
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

    def get_count_per_status(self):
        query = "SELECT COUNT(1) as Count, Status From encodingqueue GROUP BY Status ORDER BY Count"

        print query

        self.cursor.execute(query)

        rows = self.cursor.fetchall()

        result = {}

        for row in rows:
            result[row[1]] = row[0]

        return result
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
        elif packet.key == "get_count_per_status":
            self.process_request_for_count_per_status(packet)

    def process_request_for_index(self, packet):
        packet.payload['list'] = self.encodingqueue_list()
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_encode_list(self, packet):
        packet.payload['list'] = self.encodingqueue_list(profiles=packet.payload['profiles'])
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_get_next(self, packet):
        packet.payload['list'] = self.get_next(packet.payload['profiles'], packet.payload['statuses'])
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
        packet.payload['list'] = self.encodingqueue_list(statuses=packet.payload['statuses'])
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_active(self, packet):
        packet.payload['list'] = self.get_active()
        packet.return_to_sender()
        self.send(packet)

    def process_request_for_count_per_status(self, packet):
        packet.payload['count_per_status'] = self.get_count_per_status()
        packet.return_to_sender()
        self.send(packet)
