#!/usr/bin/python
import time
import urllib2
import urllib
import json
import datetime
import calendar
import subprocess
import os
import logging

# PRICE_FILE = '/global/scratch/kmuriki/bank-config.toml'
PRICE_FILE = '/etc/slurm/bank-config.toml'
# BASE_URL = 'http://scgup-dev.lbl.gov:8000/api/'
BASE_URL = 'http://mybrc.brc.berkeley.edu/mybrc-rest/'
LOG_FILE = 'updated_jobs.log'

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')

print 'starting run...'
logging.info('starting run...')


def calculate_cpu_time(duration, num_cpus):
    total_seconds = duration.total_seconds()
    hours = total_seconds / 3600
    return hours * float(num_cpus)


def to_dt_obj(dt):
    return calendar.timegm(time.strptime(dt, '%Y-%m-%dT%H:%M:%S'))  # utc


def to_dt_string(dt):
    candidate = datetime.datetime.utcfromtimestamp(dt)
    return candidate.strftime('%Y-%m-%dT%H:%M:%SZ'), candidate


def calculate_time_duration(start, end):
    return datetime.datetime.fromtimestamp(to_dt_obj(end)) - datetime.datetime.fromtimestamp(to_dt_obj(start))


def get_price_per_hour(partition):
    lines = []
    with open(PRICE_FILE, 'r') as f:
        lines = f.readlines()

    target = 0
    partition_price_passed = False
    for line in lines:
        line = line.decode('utf-8')
        if not partition_price_passed and '[PartitionPrice]' in line:
            partition_price_passed = True
            continue

        if not partition_price_passed:
            continue

        if line[0] == '#':
            continue

        if partition in line:
            target = line.split()[-1]
            target = float(target)
            break

        if '[' in line:
            break

    if target == 0:
        target = 1
    return target


def calculate_hours(duration):
    total_seconds = duration.total_seconds()
    hours = total_seconds / 3600
    return hours


def calculate_amount(partition, cpu_count, duration):
    pphr = get_price_per_hour(partition)
    duration_hrs = calculate_hours(duration)
    cpu_count = int(cpu_count)
    return round(pphr * cpu_count * duration_hrs, 2)


def node_list_format(nodelist):
    nodes = nodelist.split(',')

    table = []
    for node in nodes:
        if '-' in node:
            extension = node.split('.')[-1]
            start, end = node.split(',')[0][1:]

            for current in range(int(start), int(end) + 1):
                current = 'n{:04d}.{}'.format(current, extension)
                table.append({"name": current})

        else:
            table.append({"name": node})

    return table


def paginate_requests():
    start_ts = to_dt_obj(
        '{}-01-01T00:00:00'.format(datetime.datetime.now().year))
    current_ts = calendar.timegm(datetime.datetime.utcnow().timetuple())

    request_params = {'jobstatus': 'RUNNING',
                      'start_time': start_ts, 'end_time': current_ts}
    url_target = BASE_URL + 'jobs?' + urllib.urlencode(request_params)
    req = urllib2.Request(url_target)
    response = json.loads(urllib2.urlopen(req).read())

    current_page = 0
    job_table = []
    job_table.extend(response['results'])
    while response['next'] is not None:
        try:
            current_page += 1

            request_params = {'jobstatus': 'RUNNING', 'page': current_page,
                              'start_time': start_ts, 'end_time': current_ts}
            url_target = BASE_URL + '/jobs?' + urllib.urlencode(request_params)
            req = urllib2.Request(url_target)
            response = json.loads(urllib2.urlopen(req).read())

            job_table.extend(response['results'])
            if current_page % 5 == 0:
                print "\tgetting page: ", current_page

            if current_page > 120:
                print 'too many jobs to update at once, rerun script after this run completes...'
                logging.info(
                    'too many jobs to update at once, rerun script after this run completes...')
                break

        except urllib2.URLError:
            response['next'] = None

    return job_table


print 'gathering data from mybrcdb...'
logging.info('gathering data from mybrcdb...')

job_table = paginate_requests()
jobs = ''
for job in job_table:
    jobs += job['jobslurmid'] + '\n'

"""
with open('running_jobs.txt', 'w') as f:
    f.write(jobs)


with open('running_jobs.txt', 'r') as f:
    lines = f.readlines()

jobs = ''.join(lines)
"""

print 'gathering data from slurmdb...'
logging.info('gathering data from slurmdb...')
lines = jobs.splitlines()

central = ''
for line in lines:
    central += line.strip() + ','
central = central[:-1]

out = subprocess.Popen(['sacct', '-j', central,
                        '--format=JobIdRaw,Submit,Start,End,UID,Account,State,Partition,QOS,NodeList,AllocCPUS,ReqNodes,AllocNodes,CPUTimeRAW,CPUTime', '-n', '-P'],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


outer, _ = out.communicate()
out = outer.splitlines()

print 'parsing jobs...'
logging.info('parsing jobs...')
table = {}
param_count = 15
for current in out:
    current = current.split('|')
    current = [str(temp.decode('utf-8')) for temp in current]
    jobid, submit, start, end, uid, account, state, partition, qos, nodelist, alloc_cpus, req_nodes, alloc_nodes, cpu_time_raw, cpu_time = current

    if '.bat' in jobid:
        continue

    # if it is running in the slurmdb, skip it
    if state == 'RUNNING':
        continue

    if jobid not in jobs:
        continue

    if '.' in jobid:
        continue

    if state == 'COMPLETED':
        state = 'COMPLETING'

    duration = calculate_time_duration(start, end)
    node_list_converted = node_list_format(nodelist)
    cpu_time = calculate_cpu_time(duration, alloc_cpus)
    amount = calculate_amount(partition, alloc_cpus, duration)

    submit, _ = to_dt_string(to_dt_obj(submit))
    start, _start = to_dt_string(to_dt_obj(start))
    end, _end = to_dt_string(to_dt_obj(end))

    raw_time = (_end - _start).total_seconds() / 3600

    table[jobid] = {
        'jobslurmid': jobid,
        'submitdate': submit,
        'startdate': start,
        'enddate': end,
        'userid': uid,
        'accountid': account,
        'amount': str(amount),
        'jobstatus': state,
        'partition': partition,
        'qos': qos,
        'nodes': node_list_converted,
        'num_cpus': int(alloc_cpus),
        'num_req_nodes': int(req_nodes),
        'num_alloc_nodes': int(alloc_nodes),
        'raw_time': raw_time,
        'cpu_time': float(cpu_time)}


print 'updating ', len(table), 'jobs in mybrcdb...'
logging.info('updating mybrcdb...')

counter = 0
for jobid, job in table.items():
    request_data = urllib.urlencode(job)
    url_target = BASE_URL + 'jobs/' + str(jobid) + '/'
    req = urllib2.Request(url=url_target, data=request_data)
    req.get_method = lambda: 'PUT'

    try:
        json.loads(urllib2.urlopen(req).read())
        logging.info('{} UPDATED : {}'.format(jobid, job))
        counter += 1
    except urllib2.HTTPError, e:
        logging.warning('ERROR occured for jobid: {} REASON: {}'.format(jobid,
                                                                        e.reason))

logging.info('run complete, updated {} jobs'.format(counter))
