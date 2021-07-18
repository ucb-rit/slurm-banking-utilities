#!/usr/bin/python
import os
import time
import urllib2
import urllib
import json
import datetime
import calendar
import subprocess
import argparse
import logging

# staging is hit iff DEBUG is True
# production is hit iff DEBUG is False
DEBUG = False

PRICE_FILE = '/etc/slurm/bank-config.toml'
BASE_URL = 'http://scgup-dev.lbl.gov:8000/api/' if DEBUG else 'https://mybrc.brc.berkeley.edu/api/'
LOG_FILE = 'update_jobs_coldfront_debug.log' if DEBUG else 'update_jobs_coldfront.log'
CONFIG_FILE = 'update_jobs_coldfront.conf'

timestamp_format_complete = '%Y-%m-%dT%H:%M:%S'
timestamp_format_minimal = '%Y-%m-%d'
docstr = '''
Sync running jobs between MyBRC-DB with Slurm-DB.
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


def check_valid_date(s):
    '''check if date is in valid format(s)'''
    complete, minimal = None, None

    try:
        complete = datetime.datetime.strptime(s, timestamp_format_complete)
    except Exception:
        pass

    try:
        minimal = datetime.datetime.strptime(s, timestamp_format_minimal)
    except Exception:
        pass

    if not complete and not minimal:
        raise argparse.ArgumentTypeError('Invalid time specification {}'.format(s))
    else:
        return s


current_month = datetime.datetime.now().month
current_year = datetime.datetime.now().year
default_start = current_year if current_month >= 6 else (current_year - 1)

parser = argparse.ArgumentParser(description=docstr)
parser.add_argument('-s', dest='start', type=check_valid_date,
                    help='starttime for the query period (YYYY-MM-DD[THH:MM:SS])',
                    default='{}-06-01T00:00:00'.format(default_start))
parser.add_argument('-e', dest='end', type=check_valid_date,
                    help='endtime for the query period (YYYY-MM-DD[THH:MM:SS])',
                    default=datetime.datetime.utcnow().strftime(timestamp_format_complete))

parsed = parser.parse_args()
START = parsed.start
END = parsed.end

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


def paginate_requests():
    start_ts = datestring_to_utc_timestamp(START)
    end_ts = datestring_to_utc_timestamp(END)

    request_params = {'jobstatus': 'RUNNING',
                      'start_time': start_ts, 'end_time': end_ts}
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
                              'start_time': start_ts, 'end_time': end_ts}
            url_target = BASE_URL + '/jobs?' + urllib.urlencode(request_params)
            req = urllib2.Request(url_target)
            response = json.loads(urllib2.urlopen(req).read())

            job_table.extend(response['results'])
            if current_page % 5 == 0:
                print "\tgetting page: ", current_page

            if current_page > 120:
                print 'too many jobs to update at once, rerun script after this run completes...'
                logging.info('too many jobs to update at once, rerun script after this run completes...')
                break

        except urllib2.URLError:
            response['next'] = None

            if DEBUG:
                print '[paginate_requests()] failed: {}'.format(e)

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
                        '--format=JobId,Submit,Start,End,UID,Account,State,Partition,QOS,NodeList,AllocCPUS,ReqNodes,AllocNodes,CPUTimeRAW,CPUTime', '-n', '-P'],
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

    submit, _ = utc_timestamp_to_string(datestring_to_utc_timestamp(submit))
    start, _start = utc_timestamp_to_string(datestring_to_utc_timestamp(start))
    end, _end = utc_timestamp_to_string(datestring_to_utc_timestamp(end))

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


print 'updating', len(table), 'jobs in mybrcdb...'
logging.info('updating mybrcdb...')

counter = 0
for jobid, job in table.items():
    request_data = urllib.urlencode(job)
    url_target = BASE_URL + 'jobs/' + str(jobid) + '/'
    req = urllib2.Request(url=url_target, data=request_data)

    req.add_header('Authorization', 'Token ' + AUTH_TOKEN)
    req.get_method = lambda: 'PUT'

    try:
        json.loads(urllib2.urlopen(req).read())
        logging.info('{} UPDATED : {}'.format(jobid, job))
        counter += 1

        if counter % int(len(table) / 10) == 0:
            print '\tprogress:', counter, '/', len(table)

    except urllib2.HTTPError, e:
        logging.error('ERROR occured for jobid: {} REASON: {}'.format(jobid, e.reason))

logging.info('run complete, updated {} jobs'.format(counter))
