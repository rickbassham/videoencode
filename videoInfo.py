#!/usr/bin/python

import os
import errno
import subprocess
import json
import sys
import argparse
import signal
import sys

should_stop = False

def signal_handler(signal, frame):
    global should_stop
    print('You pressed Ctrl+C!')
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)


output = {}
combos = []

avprobe_path = '/usr/bin/avprobe'

parser = argparse.ArgumentParser(description='Dumps video information as json for use by encode script')

parser.add_argument('video_dir', help="root folder to find video info for")
parser.add_argument('output_file', help="path to the json file to output (default is stdout)", default='-')

args = parser.parse_args()

search_path = args.video_dir
output_file = args.output_file

output['search_path'] = search_path


def getStreams(path):
    command = [avprobe_path, '-loglevel', 'quiet', '-show_streams', '-of', 'json', path]
    #print " ".join(command)

    try:
        output = subprocess.check_output(command)
    except:
        return None

    return json.loads(output)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def addVideo(path):
    global combos
    root, name = os.path.split(path)

    x, extn = os.path.splitext(name)
    extn = extn.lower()
    if name.endswith('mkv') or name.endswith('avi') or name.endswith('mp4') or name.endswith('m4v'):
        video_codec = None
        audio_codec = None
        audio_channels = None
        subtitle_codec = None

        if output_file != '-':
            print name

        mi = getStreams(os.path.join(root, name))

        if mi is not None:
            mi['name'] = x
            mi['extn'] = extn
            mi['folder'] = root
            mi['encoded'] = False
            mi['path'] = os.path.join(root, name)

            combos.append(mi)

if os.path.isfile(search_path):
    addVideo(search_path)
else:
    i = 0

    for root, dirs, files in os.walk(search_path):
        for name in files:

            if should_stop:
                break

            if addVideo(os.path.join(root, name)):
                i = i + 1

#        if i > 0:
#            break

#print json.dumps(combos, sort_keys=True, indent=4)

if should_stop:
    sys.exit(1)

output['videos'] = combos

if output_file != '-':
    json.dump(output, open(output_file, 'w'), sort_keys=True, indent=4)
else:
    print json.dumps(output)
