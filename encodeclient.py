#!/usr/bin/python

# This file assumes avconv is
# ffmpeg version 2.5.4 Copyright (c) 2000-2015 the FFmpeg developers
#     configuration: --prefix=/usr --extra-cflags='-g -O2 -fstack-protector-strong -Wformat -Werror=format-security ' --extra-ldflags='-Wl,-z,relro' --cc='ccache cc' --enable-shared --enable-libmp3lame --enable-gpl --enable-nonfree --enable-libvorbis --enable-pthreads --enable-libfaac --enable-libxvid --enable-postproc --enable-x11grab --enable-libgsm --enable-libtheora --enable-libopencore-amrnb --enable-libopencore-amrwb --enable-libx264 --enable-libspeex --enable-nonfree --disable-stripping --enable-libvpx --enable-libschroedinger --disable-encoder=libschroedinger --enable-version3 --enable-libopenjpeg --enable-librtmp --enable-avfilter --enable-libfreetype --enable-libvo-aacenc --disable-decoder=amrnb --enable-libvo-amrwbenc --enable-libaacplus --libdir=/usr/lib/x86_64-linux-gnu --disable-vda --enable-libbluray --enable-libcdio --enable-gnutls --enable-frei0r --enable-openssl --enable-libass --enable-libopus --enable-fontconfig --enable-libpulse --disable-mips32r2 --disable-mipsdspr1 --disable-mipsdspr2 --enable-libvidstab --enable-libzvbi --enable-avresample --disable-htmlpages --disable-podpages --enable-libutvideo --enable-libiec61883 --enable-libfdk-aac --enable-vaapi --enable-libx265 --enable-libdc1394 --disable-altivec --shlibdir=/usr/lib/x86_64-linux-gnu
# and MP4Box is
# MP4Box - GPAC version 0.5.1-DEV-rev4065
#     configuration: --build=x86_64-linux-gnu --prefix=/usr --includedir=${prefix}/include --mandir=${prefix}/share/man --infodir=${prefix}/share/info --sysconfdir=/etc --localstatedir=/var --libdir=${prefix}/lib/x86_64-linux-gnu --libexecdir=${prefix}/lib/x86_64-linux-gnu --disable-maintainer-mode --disable-dependency-tracking --moddir=${prefix}/lib/x86_64-linux-gnu/gpac --use-js=no --extra-cflags=-fPIC -DPIC -DXP_UNIX -g -O2 -fstack-protector-strong -Wformat -Werror=format-security -D_FORTIFY_SOURCE=2 --extra-ldflags=-Wl,-z,relro -Wl,--as-needed --libdir=lib/x86_64-linux-gnu --use-freenect=yes --use-a52=system --cc=ccache cc --cpp=ccache g++

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
import logging

import interrupt

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)s %(lineno)d %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger('encodeclient')

e = interrupt.GetInterruptEvent()

server = None
exclude_video_change = False

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

    try:
        query = urllib.urlencode({ 'profile': profiles, 'status': status }, True)
        req = urllib2.Request('http://{0}:8080/get_next'.format(server), query)
        response = urllib2.urlopen(req)
        data = json.load(response)

        if len(data['list']) > 0:
            return data['list'][0]
        else:
            return None
    except:
        return None

def update_encode(rowid, status, percent_complete, framerate, encoding_reasons, error_text, encoding_start_time):
    if rowid is None:
        return

    if error_text is None:
        error_text = ''

    if encoding_reasons is None:
        encoding_reasons = ''

    now = datetime.datetime.now()

    encoding_time = (now - encoding_start_time).total_seconds()

    obj = {
        'RowID': rowid,
        'Status': status,
        'PercentComplete': percent_complete,
        'FrameRate': framerate,
        'EncodingReasons': encoding_reasons,
        'ErrorText': error_text,
        'EncodingTime': encoding_time
    }

    try:
        req = urllib2.Request('http://{0}:8080/update_encode'.format(server))
        req.add_header('Content-Type', 'application/json')
        response = urllib2.urlopen(req, json.dumps(obj))
    except:
        logger.warning('Unable to update encode.')
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
    global e
    import subprocess

    cmd = ' '.join(map(quote_if_spaces, command))

    logger.debug(cmd)

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

                indexoftime = err.find('time=')
                indexofnextwhitespace = err.find('bitrate=', indexoftime)

                current_timestamp = 0

                try:
                    time_str = err[indexoftime + 5:indexofnextwhitespace - 1]
                    time_array = time_str.split(':')

                    if len(time_array) == 3:
                        current_timestamp += int(time_array[0]) * 3600
                        current_timestamp += int(time_array[1]) * 60
                        current_timestamp += float(time_array[2])
                    else:
                        #print time_array, err[indexoftime + 5:indexofnextwhitespace]
                        pass
                except:
                    #print indexoftime, indexofnextwhitespace, err[indexoftime + 5:indexofnextwhitespace]
                    pass

                status(0, framerate, current_timestamp)

            elif err.startswith('ISO File Writing:'):
                err = err.replace('\n', '\r')
            else:
                full_error.append(err)

            sys.stdout.write(err)
            return True

        return False

    last_output = datetime.datetime.now()

    while p.poll() is None:
        if readstdout():
            last_output = datetime.datetime.now()

        if readstderr():
            last_output = datetime.datetime.now()

        if (datetime.datetime.now() - last_output).total_seconds() > 300:
            logger.error('No output in last 5 minutes, restarting encode.')
            p.kill()
            return False, 'Timeout', ''
        elif e.is_set():
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


def encode(movie, encoding_start_time, force_encode=False):
    global e
    global last_update
    # We want a video stream no wider than 1920 with Main 4.0 profile or lower
    # We want an audio stream of AAC at 160 with dolby pro logic (if coming from
    #    5.1) or stereo or mono
    # We want a single english subtitle track if it is forced or the audio
    #    language is not english

    video_stream = None
    audio_stream = None
    subtitle_stream = None

    audio_lang = None

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
        duration = 0

        if os.path.isfile(output_file):
            update_encode(movie['RowID'], 'Skipped', 0.0, 0.0, '', 'Output file already exists.', encoding_start_time)
            return False

        for stream in movie['streams']:
            if stream['codec_type'] == 'video':
                if video_stream is None:
                    video_stream = stream
                    video_stream_index = video_i
                elif stream['width'] > video_stream['width'] and stream['width'] <= 1920:
                    video_stream = stream
                    video_stream_index = video_i
                video_i += 1
            elif stream['codec_type'] == 'audio':
                tags = stream.get('tags', None)

                lang = None
                if tags is not None:
                    lang = tags.get('language', None)

                print audio_lang, lang

                if audio_stream is None:
                    audio_stream = stream
                    audio_stream_index = audio_i
                    audio_lang = lang
                elif (audio_lang != 'eng' and lang == 'eng'):
                    audio_stream = stream
                    audio_stream_index = audio_i
                    audio_lang = lang
                elif audio_lang == lang and (stream['channels'] > audio_stream['channels'] or stream.get('bit_rate', 0) > audio_stream.get('bit_rate', 0)):
                    audio_stream = stream
                    audio_stream_index = audio_i
                    audio_lang = lang
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
                    elif audio_stream_lang is not None and audio_stream_lang != 'eng' and lang == 'eng':
                        subtitle_stream = stream
                        subtitle_stream_index = subtitle_i
                subtitle_i += 1

        if subtitle_stream_index < 0:
            subtitle_stream_index = 0

        mkdir_p(os.path.dirname(output_file))

        has_srt_file = os.path.isfile(subtitle_file)

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

        command.extend([
            "-map", "0:v:{0}".format(video_stream_index),
            "-map_metadata", "-1",
            "-strict", "experimental",
        ])

        valid_profiles = ['High', 'Main', 'Baseline', 'Constrained Baseline']

        if video_stream['width'] <= 1920:
            copy_video = True
            copy_audio = True

        video_needs_encoding = False

        if not force_encode and video_stream['codec_name'] == 'h264' and video_stream['width'] <= 1920 and video_stream['level'] <= 41 and video_stream['profile'] in valid_profiles:
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
                "-filter:v:{0}".format(video_stream_index), "scale=w='min(1920\, iw):trunc(ow/a/2)*2'",
                "-x264-params", "level=41:cabac=1:vbv-maxrate=10000:vbv-bufsize=5000",
            ])

            if video_stream['codec_name'] != 'h264':
                reasons.append('video codec {0}'.format(video_stream['codec_name']))

            if video_stream['width'] > 1920:
                reasons.append('video width {0}'.format(video_stream['width']))

            if video_stream['level'] > 41:
                reasons.append('video level {0}'.format(video_stream['level']))

            if video_stream.get('profile', None) not in valid_profiles:
                reasons.append('video profile {0}'.format(video_stream.get('profile', None)))

        if not force_encode and audio_stream['channels'] == 2 and audio_stream['codec_name'] == 'aac':
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

        command.extend([
            temp_file,
        ])

        if exclude_video_change and video_needs_encoding:
            update_encode(movie['RowID'], 'PendingFull', 0.0, 0.0, '; '.join(reasons), '', encoding_start_time)
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

        duration = video_stream.get('duration', 0)
        if duration == 0:
            duration = audio_stream.get('duration', 0)

        duration = float(duration)

        update_encode(movie['RowID'], 'Encoding', 0.0, 0.0, '; '.join(reasons), '', encoding_start_time)

        def status(percent_complete, framerate, current_timestamp):
            global last_update
            now = datetime.datetime.now()

            if duration > 0 and current_timestamp > 0:
                percent_complete = current_timestamp / duration * 100

            #print current_timestamp, duration, percent_complete

            if (now - last_update).total_seconds() > 10:
                update_encode(movie['RowID'], 'Encoding', percent_complete, framerate, '; '.join(reasons), '', encoding_start_time)
                last_update = now

        success, output, error = execute(command, status)

        if success:
            command = [
                MP4Box_path,
                "-isma",
                "-inter", "500",
                temp_file
            ]

            update_encode(movie['RowID'], 'Muxing', 0.0, 0.0, '; '.join(reasons), '', encoding_start_time)

            success, output, error = execute(command)

            if success:
                logger.info('Copying to final destination: %s', output_file)

                update_encode(movie['RowID'], 'Copying', 0.0, 0.0, '; '.join(reasons), '', encoding_start_time)

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

                if has_srt_file:
                    try:
                        shutil.copy(subtitle_file, output_file.replace('.mp4', '.srt'))
                    except:
                        pass

                if not video_needs_encoding and os.path.isfile(output_file):
                    os.remove(input_file)

                update_encode(movie['RowID'], 'Complete', 0.0, 0.0, '; '.join(reasons), '', encoding_start_time)
                logger.info('Done')
                return True
            else:
                try:
                    os.remove(temp_file)
                except:
                    pass

                update_encode(movie['RowID'], 'Error', 0.0, 0.0, '; '.join(reasons), output + error, encoding_start_time)
                return False
        else:
            try:
                os.remove(temp_file)
            except:
                pass

            #if output == 'Timeout':
            #    update_encode(video['RowID'], 'Pending', 0.0, 0.0, '', '', encoding_start_time)
            #    pass
            #else:
            update_encode(movie['RowID'], 'Error', 0.0, 0.0, '; '.join(reasons), output + error, encoding_start_time)
            return False
    else:
        update_encode(movie['RowID'], 'FileNotFound', 0.0, 0.0, '', 'Input file was not found.', encoding_start_time)
        return False


def main():
    global server
    global exclude_video_change

    parser = argparse.ArgumentParser(description='Encodes videos queued at the specified server')

    parser.add_argument('server', help="ip address or hostname of the encode server")
    parser.add_argument('--exclude-video-change', help="", action="store_true")

    args = parser.parse_args()

    server = args.server
    exclude_video_change = args.exclude_video_change

    wait_time = 0.1

    while not e.wait(wait_time):
        wait_time = 10
        for video in iter(getNext, None):
            encoding_start_time = datetime.datetime.now()

            logger.info('Trying to process: %s', video['InputPath'])
            video_info = getStreams(video['InputPath'])

            if video_info is not None:
                video['streams'] = video_info['streams']

                try:
                    encode(video, encoding_start_time)
                except Exception as ex:
                    update_encode(video['RowID'], 'Exception', 0.0, 0.0, '', str(ex), encoding_start_time)
                    logger.critical(str(ex))
                    e.set()
            else:
                update_encode(video['RowID'], 'InvalidInputFile', 0.0, 0.0, '', 'Input file is not a valid video.', encoding_start_time)

            if e.is_set():
                update_encode(video['RowID'], 'Pending', 0.0, 0.0, '', '', encoding_start_time)
                break

            #print json.dumps(video, indent=4, sort_keys=True)

if __name__ == "__main__":
    main()
