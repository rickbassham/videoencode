#!/usr/bin/python

import os
import re
import errno
import signal
import sys
import shutil
import chardet
import json
import argparse

should_stop = False

def signal_handler(signal, frame):
    global should_stop
    print('You pressed Ctrl+C!')
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)


avconv_path = '/usr/bin/avconv'
MP4Box_path = '/usr/bin/MP4Box'


parser = argparse.ArgumentParser(description='Dumps video information as json for use by encode script')

parser.add_argument('input_file', help="path to the json file to read")
parser.add_argument('output_dir', help="root folder to output converted video to")
parser.add_argument('--exclude-relative-path', help="", action="store_true")

args = parser.parse_args()

converted_path = args.output_dir
include_relative_path = not args.exclude_relative_path

to_process = None

if args.input_file == '-':
    to_process = json.load(sys.stdin)
else:
    to_process = json.load(open(args.input_file))

search_path = to_process['search_path']


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
                line = stream.read(80)
                if line:
                    queue.put(line)
                else:
                    raise UnexpectedEndOfStream

        self._t = Thread(target=_populateQueue, args=(self._s, self._q))
        self._t.daemon = True
        self._t.start() #start collecting lines from the stream

    def readline(self, timeout = None):
        try:
            return self._q.get(block = timeout is not None,
                    timeout = timeout)
        except Empty:
            return None

class UnexpectedEndOfStream(Exception): pass


def quote_if_spaces(str):
    if ' ' in str:
        return '"' + str + '"'

    return str

def execute(command):
    global should_stop
    import subprocess

    cmd = ' '.join(map(quote_if_spaces, command))

    print cmd

    f = open('commands.log', 'a')
    f.write('\n')
    f.write(cmd)
    f.write('\n')
    f.write('\n')
    f.close()

    p = subprocess.Popen(command, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    nbout = NonBlockingStreamReader(p.stdout)
    nberr = NonBlockingStreamReader(p.stderr)

    full_output = []
    full_error = []

    while p.poll() is None:

        out = nbout.readline(0.1)
        err = nberr.readline(0.1)

        if out:
            sys.stdout.write(out)
            full_output.append(out)

        if err:
            sys.stdout.write(err)
            full_error.append(err)

        if should_stop:
            p.kill()

    if p.returncode != 0:
        f = open('errors.log', 'a')
        f.write(cmd)
        f.write('\n')
        f.write(''.join(full_output))
        f.write(''.join(full_error))
        f.write('\n')
        f.write('\n')
        return False

    return True

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise


def encode(movie, force_no_subs=False):
    global should_stop
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

    input_file = os.path.join(movie['folder'], movie['name'] + movie['extn'])

    if os.path.isfile(input_file):
        print movie['name']

        for stream in movie['streams']:
            if stream['codec_type'] == 'video':
                if video_stream is None:
                    video_stream = stream
                    video_stream_index = video_i
                elif stream['width'] > video_stream['width'] and stream['width'] <= 1280:
                    video_stream = stream
                    video_stream_index = video_i
                video_i = video_i + 1
            elif stream['codec_type'] == 'audio':
                if audio_stream is None:
                    audio_stream = stream
                    audio_stream_index = audio_i
                elif stream['channels'] > audio_stream['channels'] or stream.get('bit_rate', 0) > audio_stream.get('bit_rate', 0):
                    audio_stream = stream
                    audio_stream_index = audio_i
                audio_i = audio_i + 1

        for stream in movie['streams']:
            if stream['codec_type'] == 'subtitle':
                if audio_stream is not None:
                    audio_stream_lang = audio_stream['tags'].get('language', audio_stream['tags'].get('LANGUAGE', 'eng'))

                    if audio_stream_lang == 'eng' and stream['disposition']['forced'] and stream['tags']['language'] == 'eng':
                        subtitle_stream = stream
                        subtitle_stream_index = subtitle_i
                    elif audio_stream_lang != 'eng' and stream['tags']['language'] == 'eng':
                        subtitle_stream = stream
                        subtitle_stream_index = subtitle_i
                subtitle_i = subtitle_i + 1

        if subtitle_stream_index < 0:
            subtitle_stream_index = 0

        temp_file = movie['name'] + '.mp4'
        subtitle_file = os.path.join(movie['folder'], movie['name'] + '.srt')

        output_file = None

        if include_relative_path:
            output_file = os.path.join(movie['folder'], movie['name'] + '.mp4').replace(search_path, converted_path)
        else:
            output_file = os.path.join(converted_path, movie['name'] + '.mp4')

        mkdir_p(os.path.dirname(output_file))

        use_srt_file = subtitle_stream is None and os.path.isfile(subtitle_file) and not force_no_subs
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

        command = [
            avconv_path,
            "-hide_banner",
    #        "-loglevel", "error",
            "-y",
            "-i", input_file,
        ]

        if srt_file_encoding is not None:
            command.extend([
                "-sub_charenc", srt_file_encoding,
            ])

        if use_srt_file:
            command.extend([
                "-i", subtitle_file,
            ])

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


        if subtitle_stream is None and video_stream['codec_name'] == 'h264' and video_stream['width'] <= 1280 and video_stream['level'] <= 41 and video_stream['profile'] in valid_profiles:
            command.extend([
                "-codec:v:{0}".format(video_stream_index), "copy",
            ])
        else:
            command.extend([
                "-codec:v:{0}".format(video_stream_index), "libx264",
    #            "-threads", "2",
                "-crf", "22.0",
                "-profile:v", "high",
                "-filter:v:{0}".format(video_stream_index), "scale=w='min(1280\, iw):trunc(ow/a/2)*2'",
                "-x264-params", "level=41:cabac=1:vbv-maxrate=10000:vbv-bufsize=20000",
            ])

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

        if subtitle_stream is not None:
            command.extend([
                "-vf", "subtitles=filename='{0}':stream_index={1}".format(input_file, subtitle_stream_index)
            ])
        elif use_srt_file:
            command.extend([
                "-map", "1:s:{0}".format(subtitle_stream_index),
                "-c:s:{0}".format(subtitle_stream_index), "mov_text",
                '-metadata:s:s:{0}'.format(subtitle_stream_index), "language=eng",
            ])

        command.extend([
            temp_file,
        ])

        if os.path.isfile(output_file):
            return False

        success = execute(command)

        if success:
            command = [
                MP4Box_path,
                "-isma",
                "-inter", "500",
                temp_file
            ]

            success = execute(command)

            if success:
                print 'Copying to final destination...'
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

                return True
#        elif not should_stop:
#            return encode(movie, force_no_subs=True)

    return False


good = True

for video in to_process['videos']:
    if should_stop:
        print 'Should Stop!!'
        break

    good = encode(video) and good

    #break

if not good:
    sys.exit(1)

sys.exit(0)
