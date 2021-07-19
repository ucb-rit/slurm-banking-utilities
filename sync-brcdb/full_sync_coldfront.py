#!/usr/bin/python
import os
import time
import urllib2
import urllib
import json
import datetime
import calendar
import subprocess
import logging

# staging is hit iff DEBUG is True
# production is hit iff DEBUG is False
DEBUG = False

PRICE_FILE = '/etc/slurm/bank-config.toml'
BASE_URL = 'http://scgup-dev.lbl.gov:8000/api/' if DEBUG else 'https://mybrc.brc.berkeley.edu/api/'
LOG_FILE = 'full_sync_coldfront_debug.log' if DEBUG else 'full_sync_coldfront.log'
CONFIG_FILE = 'full_sync_coldfront.conf'

timestamp_format_complete = '%Y-%m-%dT%H:%M:%S'
timestamp_format_minimal = '%Y-%m-%d'
docstr = '''
Full Sync jobs between MyBRC-DB with Slurm-DB.
'''


logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')

if not os.path.exists(CONFIG_FILE):
    print 'config file {} missing...'.format(CONFIG_FILE)
    logging.info('auth config file missing [{}], exiting run...'.format(CONFIG_FILE))
    exit(0)

with open(CONFIG_FILE, 'r') as f:
    AUTH_TOKEN = f.read().strip()

print 'starting run...'
logging.info('starting run...')


def calculate_cpu_time(duration, num_cpus):
    total_seconds = duration.total_seconds()
    hours = total_seconds / 3600
    return hours * float(num_cpus)


def datestring_to_utc_timestamp(dt):
    try:
        return calendar.timegm(time.strptime(dt, timestamp_format_complete))
    except:
        return calendar.timegm(time.strptime(dt, timestamp_format_minimal))


def utc_timestamp_to_string(dt):
    candidate = datetime.datetime.utcfromtimestamp(dt)
    return candidate.strftime('%Y-%m-%dT%H:%M:%SZ'), candidate


def calculate_time_duration(start, end):
    return datetime.datetime.utcfromtimestamp(datestring_to_utc_timestamp(end)) - datetime.datetime.utcfromtimestamp(datestring_to_utc_timestamp(start))


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


def paginate_requests(url, params=None):
    request_url = url
    params = params or {}

    if params:
        request_url = url + '?' + urllib.urlencode(params)

    try:
        req = urllib2.Request(request_url)
        response = json.loads(urllib2.urlopen(req).read())
    except urllib2.URLError, e:
        logging.error('[paginate_requests({}, {})] failed: {}'.format(url, params, e))

        if DEBUG:
            print '[paginate_requests({}, {})] failed: {}'.format(url, params, e)

        return []

    current_page = 1
    results = []
    results.extend(response['results'])
    while response['next'] is not None:
        try:
            params['page'] = current_page
            request_url = url + '?' + urllib.urlencode(params)
            req = urllib2.Request(request_url)
            response = json.loads(urllib2.urlopen(req).read())
            results.extend(response['results'])

            current_page += 1
            if current_page > 120:
                logging.warning('too many pages to sync at once, rerun script after this run completes...')
                print 'too many pages to sync at once, rerun script after this run completes...'
                break

        except urllib2.URLError, e:
            response['next'] = None
            logging.error('[paginate_requests({}, {})] failed: {}'.format(url, params, e))

            if DEBUG:
                print '[paginate_requests({}, {})] failed: {}'.format(url, params, e)

    return results


def single_request(url, params=None):
    request_url = url
    params = params or {}

    if params:
        request_url = url + '?' + urllib.urlencode(params)

    try:
        request = urllib2.Request(request_url)
        response = json.loads(urllib2.urlopen(request).read())
    except Exception, e:
        response = {'results': None}
        logging.error('[single_request({}, {})] failed: {}'.format(url, params, e))

        if DEBUG:
            print '[single_request({}, {})] failed: {}'.format(url, params, e)

    return response['results']


def get_project_start(project):
    allocations_url = BASE_URL + 'allocations/'
    response = single_request(allocations_url, {'project': project, 'resources': 'Savio Compute'})
    if not response or len(response) == 0:
        if DEBUG:
            print '[get_project_start({})] ERR'.format(project)

        logging.error('[get_project_start({})] ERR'.format(project))
        return None

    creation = response[0]['start_date']
    return creation.split('.')[0] if '.' in creation else creation


print 'gathering accounts from mybrcdb...'

project_table = []
project_table_unfiltered = paginate_requests(BASE_URL + 'projects/')
for project in project_table_unfiltered:
    project_name = str(project['name'])
    project_start = get_project_start(project_name)

    project['name'] = project_name

    if project_start:
        project['start'] = str(project_start)
        project_table.append(project)

print 'gathering jobs from slurmdb'

for project in project_table:
    out, err = subprocess.Popen(['sacct', '-A', project['name'], '-S', project['start'],
                                 '--format=JobId,Submit,Start,End,UID,Account,State,Partition,QOS,NodeList,AllocCPUS,ReqNodes,AllocNodes,CPUTimeRAW,CPUTime', '-naPX'],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()
    project['jobs'] = out.splitlines()

print 'parsing jobs...'

job_table = {}
for project in project_table:
    for line in project['jobs']:
        values = [str(value.decode('utf-8')) for value in line.split('|')]
        jobid, submit, start, end, uid, account, state, partition, qos, nodelist, alloc_cpus, req_nodes, alloc_nodes, cpu_time_raw, cpu_time = values

        try:
            duration = calculate_time_duration(start, end)
            node_list_converted = node_list_format(nodelist)
            cpu_time = calculate_cpu_time(duration, alloc_cpus)
            amount = calculate_amount(partition, alloc_cpus, duration)

            submit, _ = utc_timestamp_to_string(datestring_to_utc_timestamp(submit))
            start, _start = utc_timestamp_to_string(datestring_to_utc_timestamp(start))
            end, _end = utc_timestamp_to_string(datestring_to_utc_timestamp(end))

            raw_time = (_end - _start).total_seconds() / 3600

            job_table[jobid] = {
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
        except Exception, e:
            logging.warning('ERROR occured for jobid: {} REASON: {}'.format(jobid, e))


print 'pushing/updating', len(job_table), 'jobs in mybrcdb...'

counter = 0
for jobid, job in job_table.items():
    request_data = urllib.urlencode(job)
    url_target = BASE_URL + 'jobs/' + str(jobid) + '/'
    req = urllib2.Request(url=url_target, data=request_data)

    req.add_header('Authorization', 'Token ' + AUTH_TOKEN)
    req.get_method = lambda: 'PUT'

    try:
        json.loads(urllib2.urlopen(req).read())
        logging.info('{} PUSHED/UPDATED : {}'.format(jobid, job))
        counter += 1

        if counter % int(len(job_table) / 10) == 0:
            print '\tprogress:', counter, '/', len(job_table)

    except urllib2.HTTPError, e:
        logging.warning('ERROR occured for jobid: {} REASON: {}'.format(jobid, e.reason))

print 'run complete, pushed/updated', counter, 'jobs'
logging.info('run complete, pushed/updated {} jobs'.format(counter))
