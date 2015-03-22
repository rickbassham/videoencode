import signal
import sys
from threading import Event

def GetInterruptEvent():
    e = Event()

    def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        e.set()

    signal.signal(signal.SIGINT, signal_handler)

    return e
