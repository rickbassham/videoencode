#!/usr/bin/python

import json

from Queue import Queue, Empty
from threading import Thread, Event

class Packet(object):
    def __init__(self, key, to_manager, from_manager, payload={}):
        self.key = key
        self.to_manager = to_manager
        self.from_manager = from_manager
        self.payload = payload

    def return_to_sender(self):
        old_to = self.to_manager
        self.to_manager = self.from_manager
        self.from_manager = old_to


class Manager(Thread):
    def __init__(self, name):
        Thread.__init__(self)
        self.name = name
        self.incoming = Queue()
        self.connected_managers = {}
        self._event = Event()

    def connect_to(self, manager):
        self.connected_managers[manager.name] = manager

    def send(self, packet):
        self.send_to(packet, packet.to_manager)

    def send_to(self, packet, destination_manager):
        mgr = self.connected_managers.get(destination_manager, None)

        if mgr is not None:
            mgr.incoming.put(packet)
        else:
            raise Exception('Manager not found: {0}'.format(destination_manager))

    def stop(self):
        self._event.set()
        self.join()

    def run(self):
        self.starting()

        while not self._event.wait(0.1):
            try:
                self.dowork()
            except Exception as e:
                print e
                self._event.set()

        self.stopping()

    def starting(self):
        pass

    def dowork(self):
        try:
            pkt = self.incoming.get(False)

            if pkt.to_manager == self.name:
                self.process(pkt)
            else:
                self.send(pkt)

        except Empty:
            pass

    def stopping(self):
        pass

    def process(self, packet):
        print json.dumps(packet.__dict__)
