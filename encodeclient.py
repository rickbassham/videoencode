#!/usr/bin/python

import argparse
import chardet
import datetime
import errno
import json
import os
import re
import shutil
import signal
import subprocess
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

parser = argparse.ArgumentParser(description='Encodes videos queued at the specified server')

parser.add_argument('server', help="ip address or hostname of the encode server")
parser.add_argument('--exclude-video-change', help="", action="store_true")

args = parser.parse_args()

server = args.server
exclude_video_change = args.exclude_video_change


current_row_id = None
last_update = datetime.datetime.now()

avprobe_path = '/usr/bin/avprobe'
avconv_path = '/usr/bin/avconv'
MP4Box_path = '/usr/bin/MP4Box'

profiles = ['FireTV', 'TestProfile']

def getStreams(path):
    command = [avprobe_path, '-loglevel', 'quiet', '-show_streams', '-of', 'json', path]
    #print " ".join(command)

    try:
        output = subprocess.check_output(command)
    except:
        return None

    return json.loads(output)

def getNext():
    global exclude_video_change
    status = ['Pending']

    if not exclude_video_change:
        status.append('PendingFull')

    query = urllib.urlencode({ 'profile': profiles, 'status': status }, True)
    req = urllib2.Request('http://{0}:8080/get_next'.format(server), query)
    response = urllib2.urlopen(req)
    data = json.load(response)

    if len(data['list']) > 0:
        return data['list'][0]
    else:
        return None

def update_encode(rowid, status, percent_complete, framerate, encoding_reasons, error_text):
    if error_text is None:
        error_text = ''

    if encoding_reasons is None:
        encoding_reasons = ''

    obj = {
        'RowID': rowid,
        'Status': status,
        'PercentComplete': percent_complete,
        'FrameRate': framerate,
        'EncodingReasons': encoding_reasons,
        'ErrorText': error_text
    }

    try:
        req = urllib2.Request('http://{0}:8080/update_encode'.format(server))
        req.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(req, json.dumps(obj))
    except:
        print 'Unable to update encode.'
        pass

from threading import Thread
from Queue import Queue, Empty

class NonBlockingStreamReader:

    def __init__(self, stream):
        '''
        stream: the stream to read from.
                Usually a process' stdout or stderr.
        '''

        self._s = stream
        self._q = Queue()

        def _populateQueue(stream, queue):
            '''
            Collect lines from 'stream' and put them in 'quque'.
            '''

            while True:
                line = stream.readline()
                if line:
                    queue.put(line)
                else:
                    break

        self._t = Thread(target=_populateQueue, args=(self._s, self._q))
        self._t.daemon = True
        self._t.start() #start collecting lines from the stream

    def readline(self, timeout=None):
        try:
            return self._q.get(block=timeout is not None, timeout=timeout)
        except Empty:
            return None

class UnexpectedEndOfStream(Exception): pass


def quote_if_spaces(str):
    if ' ' in str:
        return '"' + str + '"'

    return str

def execute(command, status=None):
    global should_stop
    import subprocess

    cmd = ' '.join(map(quote_if_spaces, command))

    print cmd

    f = open('commands.log', 'a')
    f.write(datetime.datetime.now().isoformat())
    f.write('\n')
    f.write(cmd.encode('UTF-8'))
    f.write('\n')
    f.write('\n')
    f.write('\n')
    f.close()

    p = subprocess.Popen(command, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    nbout = NonBlockingStreamReader(p.stdout)
    nberr = NonBlockingStreamReader(p.stderr)

    full_output = []
    full_error = []


    def readstdout():
        out = nbout.readline(0.1)

        if out:
            sys.stdout.write(out)
            full_output.append(out)
            return True

        return False


    def readstderr():
        err = nberr.readline(0.1)

        if err:
            if err.startswith('frame='):
                err = err.replace('\n', '\r')

                indexoffps = err.find('fps=')
                indexofnextwhitespace = err.find('q=', indexoffps)

                framerate = 0.0

                try:
                    framerate = float(err[indexoffps + 4:indexofnextwhitespace - 1])
                except:
                    #print err[indexoffps + 4:indexofnextwhitespace]
                    pass

                status(0.0, framerate)

            else:
                full_error.append(err)

            sys.stdout.write(err)
            return True

        return False


    while p.poll() is None:
        readstdout()
        readstderr()

        if should_stop:
            p.kill()


    while readstdout():
        pass

    while readstderr():
        pass


    if p.returncode != 0:
        f = open('errors.log', 'a')
        f.write(datetime.datetime.now().isoformat())
        f.write('\n')
        f.write(cmd.encode('UTF-8'))
        f.write('\n')
        f.write(''.join(full_output))
        f.write(''.join(full_error))
        f.write('\n')
        f.write('\n')
        return False, ''.join(full_output), ''.join(full_error)

    return True, None, None

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise


def encode(movie):
    global should_stop
    global last_update
    # We want a video stream no wider than 1280 with Main 4.0 profile or lower
    # We want an audio stream of AAC at 160 with dolby pro logic (if coming from
    #    5.1) or stereo or mono
    # We want a single english subtitle track if it is forced or the audio
    #    language is not english

    video_stream = None
    audio_stream = None
    subtitle_stream = None

    video_stream_index = -1
    audio_stream_index = -1
    subtitle_stream_index = -1

    video_i = 0
    audio_i = 0
    subtitle_i = 0

    input_file = movie['InputPath']

    root, input_file_name = os.path.split(input_file)

    name, extn = os.path.splitext(input_file_name)
    extn = extn.lower()

    if os.path.isfile(input_file):

        temp_file = name + '.mp4'
        subtitle_file = os.path.join(root, name + '.srt')
        output_file = movie['OutputPath']

        if os.path.isfile(output_file):
            update_encode(movie['RowID'], 'Skipped', 0.0, 0.0, '', 'Output file already exists.')
            return False

        for stream in movie['streams']:
            if stream['codec_type'] == 'video':
                if video_stream is None:
                    video_stream = stream
                    video_stream_index = video_i
                elif stream['width'] > video_stream['width'] and stream['width'] <= 1280:
                    video_stream = stream
                    video_stream_index = video_i
                video_i += 1
            elif stream['codec_type'] == 'audio':
                if audio_stream is None:
                    audio_stream = stream
                    audio_stream_index = audio_i
                elif stream['channels'] > audio_stream['channels'] or stream.get('bit_rate', 0) > audio_stream.get('bit_rate', 0):
                    audio_stream = stream
                    audio_stream_index = audio_i
                audio_i += 1

        for stream in movie['streams']:
            if stream['codec_type'] == 'subtitle':
                if audio_stream is not None:
                    tags = audio_stream.get('tags', None)
                    audio_stream_lang = None
                    if tags is not None:
                        audio_stream_lang = tags.get('language', tags.get('LANGUAGE', 'eng'))

                    tags = stream.get('tags', None)
                    lang = None
                    if tags is not None:
                        lang = tags.get('language', tags.get('LANGUAGE', 'eng'))

                    if audio_stream_lang == 'eng' and stream['disposition']['forced'] and stream['tags']['language'] == 'eng':
                        subtitle_stream = stream
                        subtitle_stream_index = subtitle_i
                    elif audio_stream_lang != 'eng' and lang == 'eng':
                        subtitle_stream = stream
                        subtitle_stream_index = subtitle_i
                subtitle_i += 1

        if subtitle_stream_index < 0:
            subtitle_stream_index = 0

        mkdir_p(os.path.dirname(output_file))

        use_srt_file = subtitle_stream is None and os.path.isfile(subtitle_file)
        srt_file_encoding = None

        if use_srt_file:
            raw = open(subtitle_file).read()
            srt_file_encoding = chardet.detect(raw)['encoding']

            if srt_file_encoding.startswith('UTF-8'):
                srt_file_encoding = 'UTF-8'
            elif srt_file_encoding.startswith('UTF-16'):
                srt_file_encoding = 'UTF-16'

        copy_video = False
        copy_audio = False

        reasons = []

        command = [
            avconv_path,
            "-hide_banner",
    #        "-loglevel", "error",
            "-y",
            "-i", input_file,
        ]

        if use_srt_file:
            if srt_file_encoding is not None:
                command.extend([
                    "-sub_charenc", srt_file_encoding,
                ])

            command.extend([
                "-i", subtitle_file,
            ])
            reasons.append('Add SRT file')

        command.extend([
            "-map", "0:v:{0}".format(video_stream_index),
            "-map_metadata", "-1",
            "-strict", "experimental",
        ])

        valid_profiles = ['High', 'Main', 'Baseline', 'Constrained Baseline']

        if video_stream['width'] <= 1280:
            copy_video = True

        if audio_stream['channels'] <= 2:
            copy_audio = True

        video_needs_encoding = False

        if subtitle_stream is None and video_stream['codec_name'] == 'h264' and video_stream['width'] <= 1280 and video_stream['level'] <= 41 and video_stream['profile'] in valid_profiles:
            command.extend([
                "-codec:v:{0}".format(video_stream_index), "copy",
            ])
        else:
            video_needs_encoding = True
            command.extend([
                "-codec:v:{0}".format(video_stream_index), "libx264",
    #            "-threads", "2",
                "-crf", "22.0",
                "-profile:v", "high",
                "-filter:v:{0}".format(video_stream_index), "scale=w='min(1280\, iw):trunc(ow/a/2)*2'",
                "-x264-params", "level=41:cabac=1:vbv-maxrate=10000:vbv-bufsize=20000",
            ])

            if video_stream['codec_name'] != 'h264':
                reasons.append('video codec {0}'.format(video_stream['codec_name']))

            if video_stream['width'] > 1280:
                reasons.append('video width {0}'.format(video_stream['width']))

            if video_stream['level'] > 41:
                reasons.append('video level {0}'.format(video_stream['level']))

            if video_stream.get('profile', None) not in valid_profiles:
                reasons.append('video profile {0}'.format(video_stream.get('profile', None)))

        if audio_stream['channels'] == 2 and audio_stream['codec_name'] == 'aac':
            command.extend([
                "-map", "0:a:{0}".format(audio_stream_index),
                "-codec:a:{0}".format(audio_stream_index), "copy",
            ])
        else:
            command.extend([
                "-map", "0:a:{0}".format(audio_stream_index),
                "-codec:a:{0}".format(audio_stream_index), "aac",
                "-b:a:{0}".format(audio_stream_index), "160k",
            ])

            if audio_stream['channels'] > 2:
                command.extend([
                    "-ac", "2",
                    "-af", "aresample=matrix_encoding=dplii",
                ])

                reasons.append('audio channels {0}'.format(audio_stream['channels']))

            if audio_stream['codec_name'] != 'aac':
                reasons.append('audio codec {0}'.format(audio_stream['codec_name']))

        if subtitle_stream is not None:
            command.extend([
                "-vf", "subtitles=filename='{0}':stream_index={1}".format(input_file, subtitle_stream_index)
            ])

            reasons.append('forced subtitles')

        elif use_srt_file:
            command.extend([
                "-map", "1:s:{0}".format(subtitle_stream_index),
                "-c:s:{0}".format(subtitle_stream_index), "mov_text",
                '-metadata:s:s:{0}'.format(subtitle_stream_index), "language=eng",
            ])

        command.extend([
            temp_file,
        ])

        if exclude_video_change and video_needs_encoding:
            update_encode(movie['RowID'], 'PendingFull', 0.0, 0.0, '; '.join(reasons), '')
            return False

        f = open('reasons.log', 'a')
        f.write(datetime.datetime.now().isoformat())
        f.write(' ')
        f.write(name.encode('UTF-8'))
        f.write('\n')
        f.write('; '.join(reasons))
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.close()

        update_encode(movie['RowID'], 'Encoding', 0.0, 0.0, '; '.join(reasons), '')

        def status(percent_complete, framerate):
            global last_update
            now = datetime.datetime.now()

            if (now - last_update).total_seconds() > 10:
                update_encode(movie['RowID'], 'Encoding', percent_complete, framerate, '; '.join(reasons), '')
                last_update = now

        success, output, error = execute(command, status)

        if success:
            command = [
                MP4Box_path,
                "-isma",
                "-inter", "500",
                temp_file
            ]

            update_encode(movie['RowID'], 'Muxing', 0.0, 0.0, '; '.join(reasons), '')

            success, output, error = execute(command)

            if success:
                print 'Copying to final destination...', output_file

                update_encode(movie['RowID'], 'Copying', 0.0, 0.0, '; '.join(reasons), '')

                try:
                    shutil.move(temp_file, output_file + '.tmp')
                except:
                    pass

                try:
                    shutil.move(output_file + '.tmp', output_file)
                except:
                    pass

                try:
                    os.remove(temp_file)
                except:
                    pass

                if copy_video and copy_audio and os.path.isfile(output_file):
                    os.remove(input_file)

                update_encode(movie['RowID'], 'Complete', 0.0, 0.0, '; '.join(reasons), '')
                return True
            else:
                update_encode(movie['RowID'], 'Error', 0.0, 0.0, '; '.join(reasons), output + error)
                return False
        else:
            update_encode(movie['RowID'], 'Error', 0.0, 0.0, '; '.join(reasons), output + error)
            return False
    else:
        update_encode(movie['RowID'], 'FileNotFound', 0.0, 0.0, '', 'Input file was not found.')
        return False


for video in iter(getNext, None):
    print video['InputPath']
    video_info = getStreams(video['InputPath'])

    if video_info is not None:
        video['streams'] = video_info['streams']

        try:
            encode(video)
        except Exception as e:
            update_encode(video['RowID'], 'Exception', 0.0, 0.0, '', str(e))
            print str(e)
    else:
        update_encode(video['RowID'], 'InvalidInputFile', 0.0, 0.0, '', 'Input file is not a valid video.')

    if should_stop:
        break

    #print json.dumps(video, indent=4, sort_keys=True)
