#!/usr/bin/python

from datamanager import DataManager
from requestmanager import RequestManager

def Application():
    import interrupt
    e = interrupt.GetInterruptEvent()

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
