#!/usr/bin/python

import json
import time
import urllib
import urllib2

def add_encode():
    obj = {
        'CreatedTimestamp': int(time.time()),
        'LastUpdatedTimestamp': int(time.time()),
        'Profile': 'FireTV',
        'Priority': 1,
        'Status': 'Pending',
        'ShouldStop': False,
        'PercentComplete': 0.0,
        'FrameRate': 0.0,
        'InputPath': '/mnt/public/Videos/Movies/12 Years a Slave (2013)/12 Years a Slave (2013).mkv',
        'OutputPath': '/mnt/public/ConvertedVideos/Movies/12 Years a Slave (2013)/12 Years a Slave (2013).mp4'
    }

    req = urllib2.Request('http://localhost:8080/add_encode')
    req.add_header('Content-Type', 'application/json')

    response = urllib2.urlopen(req, json.dumps(obj))

    print response

def update_encode():
    obj = {
        'RowID': 184,
        'Status': 'Pending',
        'PercentComplete': 0,
        'FrameRate': 0
    }

    req = urllib2.Request('http://localhost:8080/update_encode')
    req.add_header('Content-Type', 'application/json')

    response = urllib2.urlopen(req, json.dumps(obj))

    print response

def reset_to_pending():
    statuses = [ 'Starting', 'InvalidInputFile', 'Error' ]
    query = urllib.urlencode({ 'status': statuses }, True)
    req = urllib2.Request('http://localhost:8080/reset_to_pending', query)
    response = urllib2.urlopen(req)
    data = json.load(response)
    print data

reset_to_pending()
