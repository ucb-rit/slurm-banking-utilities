#!/usr/bin/python2
import urllib2
import urllib
import json
import time
import string
from collections import defaultdict
import os


VERSION = 0.1
docstr = '''
[version: {}]
'''.format(VERSION)

BASE_URL = 'http://mybrc.brc.berkeley.edu/mybrc-rest/'
# BASE_URL = 'https://scgup-dev.lbl.gov:8443/mybrc-rest'
# BASE_URL = 'http://localhost:8880/mybrc-rest'

FILE_NAME = 'jobcomp.log'

timestamp_format = '%Y-%m-%dT%H:%M:%S'


def process_date_time(date_time):
    return time.mktime(time.strptime(date_time, timestamp_format)) if date_time else None


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def get_job_url(start, end, user, account, page=1):
    request_params = {
        'page': page
    }

    if start:
        request_params['start_time'] = start

    if end:
        request_params['end_time'] = end

    if user:
        request_params['user'] = user

    if account:
        request_params['account'] = account

    url_usages = BASE_URL + '/jobs?' + \
        urllib.urlencode(request_params)
    return url_usages


def paginate_req_table(url_function, params=[None, None, None, None]):
    req = urllib2.Request(url_function(*params))
    response = json.loads(urllib2.urlopen(req).read())

    table = response['results']
    page = 2
    while response['next'] is not None:
        try:
            req = urllib2.Request(url_function(*params, page=page))
            response = json.loads(urllib2.urlopen(req).read())

            yield response['results']
            # table.extend(response['results'])
            page += 1
        except urllib2.URLError:
            response['next'] = None

    # return table


def calculate_params():
    with open(FILE_NAME, 'r') as f:
        lines = f.read().splitlines()
        if len(lines) > 0:
            blobs = lines[-1].split()
        else:
            return [None, None, None, None]

    last_start_time = None
    for blob in blobs:
        if 'StartTime=' in blob:
            last_start_time = blob.split('=')[-1]
    return [last_start_time, None, None, None]


def guard(params, param):
    try:
        ret = params[param]
    except TypeError:
        ret = None

    return ret


params = [None, None, None, None]
if os.path.isfile(FILE_NAME):
    params = calculate_params()

line_template = '''JobId={jobid} UserId={userid} JobState={jobstate} Partition={partition} StartTime={starttime} EndTime={endtime} NodeList={nodelist} NodeCnt={nodecount} ProcCnt={proccount} QOS={qos} SubmitTime={submittime}'''

with open(FILE_NAME, 'a') as f:
    for batch in paginate_req_table(get_job_url, params):
        for job in batch:
            jobid = guard(job, 'jobslurmid')
            userid = guard(job, 'userid')
            jobstate = guard(job, 'jobstatus')
            partition = guard(job, 'partition')
            nodelist = guard(job, 'nodes')
            nodecnt = guard(job, 'num_alloc_nodes')
            proccnt = guard(job, 'num_cpus')
            qos = guard(job, 'qos')
            starttime = guard(job, 'startdate')
            endtime = guard(job, 'enddate')
            submittime = guard(job, 'submitdate')

            starttime = None if not starttime else process_date_time(starttime[:-1])
            endtime = None if not endtime else process_date_time(endtime[:-1])
            submittime = None if not submittime else process_date_time(submittime[:-1])

            nodelist = [elem['name'] for elem in nodelist]

            f.write(string.Formatter()
                    .vformat(line_template, (),
                             SafeDict(jobid=jobid,
                                      userid=userid,
                                      jobstate=jobstate,
                                      partition=partition,
                                      starttime=starttime, endtime=endtime,
                                      nodelist=nodelist,
                                      nodecount=nodecnt, proccount=proccnt,
                                      qos=qos,
                                      submittime=submittime)) + '\n')

            print string.Formatter() \
                .vformat(line_template, (),
                         SafeDict(jobid=jobid,
                                  userid=userid,
                                  jobstate=jobstate,
                                  partition=partition,
                                  starttime=starttime, endtime=endtime,
                                  nodelist=nodelist,
                                  nodecount=nodecnt, proccount=proccnt,
                                  qos=qos,
                                  submittime=submittime))
