#!/usr/bin/python

import os
import errno
import subprocess

import json

combos = []

search_path = '/media/psf/storage/Videos/Movies/'
avprobe_path = '/usr/bin/avprobe'

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

i = 0

for root, dirs, files in os.walk(search_path):
    for name in files:
        x, extn = os.path.splitext(name)
        extn = extn.lower()
        if name.endswith('mkv') or name.endswith('avi') or name.endswith('mp4') or name.endswith('m4v'):
            video_codec = None
            audio_codec = None
            audio_channels = None
            subtitle_codec = None

            print name

            mi = getStreams(os.path.join(root, name))

            if mi is not None:
                mi['name'] = x
                mi['extn'] = extn
                mi['folder'] = root
                mi['encoded'] = False

                combos.append(mi)

                i = i + 1

#    if i > 0:
#        break

#print json.dumps(combos, sort_keys=True, indent=4)

json.dump(combos, open('movies.json', 'w'), sort_keys=True, indent=4)
