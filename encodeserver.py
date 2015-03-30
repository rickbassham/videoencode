#!/usr/bin/python

from datamanager import DataManager
from requestmanager import RequestManager

def Application():
    import interrupt
    e = interrupt.GetInterruptEvent()

    while not e.wait(0.5):
        managers = []

        mgr1 = DataManager()
        mgr2 = RequestManager()

        mgr1.connect_to(mgr2)
        mgr2.connect_to(mgr1)

        managers.append(mgr1)
        managers.append(mgr2)

        for mgr in managers:
            mgr.start()

        print 'In main run loops...'

        while not e.wait(0.5):
            for mgr in managers:
                if not mgr.is_alive():
                    print mgr.name, 'crashed. Restarting...'
            pass

        print 'Stopping managers...'

        for mgr in managers:
            mgr.stop()


if __name__ == '__main__':
    Application()
