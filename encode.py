#!/usr/bin/python

import os
import errno
import sys
import subprocess
import shutil

import json

avconv_path = '/usr/bin/avconv'
MP4Box_path = '/usr/bin/MP4Box'

search_path = '/media/psf/storage/Videos/Movies/'
converted_path = '/media/psf/storage/ConvertedVideos/Movies/'


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise


def encode(movie):
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

    input_file = os.path.join(movie['folder'], movie['name'] + movie['extn'])
    temp_file = movie['name'] + '.mp4'
    output_file = os.path.join(movie['folder'], movie['name'] + '.mp4').replace(search_path, converted_path)
    subtitle_file = os.path.join(movie['folder'], movie['name'] + '.srt')

    mkdir_p(os.path.dirname(output_file))

    use_srt_file = subtitle_stream is None and os.path.isfile(subtitle_file)

    command = [
        avconv_path,
        "-hide_banner",
#        "-loglevel", "error",
        "-y",
        "-i", input_file,
    ]

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

    if subtitle_stream is None and video_stream['codec_name'] == 'h264' and video_stream['width'] <= 1280 and video_stream['level'] <= 41 and video_stream['profile'] in valid_profiles:
        command.extend([
            "-codec:v:{0}".format(video_stream_index), "copy",
        ])
    else:
        command.extend([
            "-codec:v:{0}".format(video_stream_index), "libx264",
            "-threads", "2",
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
            "-vf", 'subtitles=filename={0}:stream_index={1}'.format(input_file, subtitle_stream_index)
        ])
    elif use_srt_file:
        command.extend([
            "-map", "1:s:{0}".format(subtitle_stream_index),
            "-c:s:{0}".format(subtitle_stream_index), "mov_text",
            "-metadata:s:s:{0}".format(subtitle_stream_index), "language=eng",
        ])

    command.extend([
        temp_file,
    ])

    if os.path.isfile(output_file):
        return

    print ' '.join(command)

    ret = subprocess.call(command)

    if ret == 0:
        command = [
            MP4Box_path,
            "-isma",
            "-inter", "500",
            temp_file
        ]

        ret = subprocess.call(command)

        if ret == 0:
            print 'Copying to final destination...'
            shutil.move(temp_file, output_file + '.tmp')
            shutil.move(output_file + '.tmp', output_file)
        else:
            f = open('errors.log', 'a')
            f.write(movie['name'])
            f.write('\n')
            f.write(' '.join(command))
            f.write('\n')
    else:
        f = open('errors.log', 'a')
        f.write(movie['name'])
        f.write('\n')
        f.write(' '.join(command))
        f.write('\n')

if (len(sys.argv) > 1):
    fp = sys.argv[1]
else:
    fp = 'movies.json'

movies = json.load(open(fp))

#print json.dumps(movies, indent=3)

for movie in movies:
    encode(movie)
    #break
