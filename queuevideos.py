#!/usr/bin/python

import os
import errno
import subprocess
import json
import sys
import argparse
import signal
import sys
import time
import urllib
import urllib2

should_stop = False

def signal_handler(signal, frame):
    global should_stop
    print('You pressed Ctrl+C!')
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)

parser = argparse.ArgumentParser(description='Adds videos to encode server queue')

parser.add_argument('video_dir', help="root folder to find video info for")
parser.add_argument('output_dir', help="root folder to output converted video to")
parser.add_argument('--exclude-relative-path', help="", action="store_true")

args = parser.parse_args()

search_path = args.video_dir
converted_path = args.output_dir
include_relative_path = not args.exclude_relative_path

def addVideo(path):
    global combos
    root, name = os.path.split(path)

    x, extn = os.path.splitext(name)
    extn = extn.lower()

    if name.endswith('mkv') or name.endswith('avi') or name.endswith('mp4') or name.endswith('m4v'):
        input_file = os.path.join(root, name)
        output_file = None

        if include_relative_path:
            output_file = os.path.join(root, x + '.mp4').replace(search_path, converted_path)
        else:
            output_file = os.path.join(converted_path, x + '.mp4')

        obj = {
            'CreatedTimestamp': int(time.time()),
            'LastUpdatedTimestamp': int(time.time()),
            'Profile': 'FireTV',
            'Priority': 1,
            'Status': 'Pending',
            'ShouldStop': False,
            'PercentComplete': 0.0,
            'FrameRate': 0.0,
            'InputPath': input_file,
            'OutputPath': output_file,
            'ErrorText': '',
            'EncodingReasons', ''
        }

        print input_file

        req = urllib2.Request('http://localhost:8080/add_encode')
        req.add_header('Content-Type', 'application/json')

        response = urllib2.urlopen(req, json.dumps(obj))

        #print response

if os.path.isfile(search_path):
    addVideo(search_path)
else:
    i = 0

    for root, dirs, files in os.walk(search_path):
        for name in files:

            if should_stop:
                break

            if addVideo(os.path.join(root, name)):
                i += 1
