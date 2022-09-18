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
import datetime
import logging


docstr = '''
Sync running jobs between MyBRC/MyLRC DB with Slurm-DB.
By Default, will launch in DEBUG mode where data is only collected and logged, not PUSHED upstream.
To actually update data upstream, look at the --PUSH flag.
'''


timestamp_format_complete = '%Y-%m-%dT%H:%M:%S'
timestamp_format_minimal = '%Y-%m-%d'
MODE_MYBRC = 'mybrc'
MODE_MYLRC = 'mylrc'


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


# date time string -> time stamp
def to_timestamp(date_time, to_utc=False):
    try:
        dt_obj = datetime.datetime.strptime(date_time, timestamp_format_complete)
    except ValueError:
        dt_obj = datetime.datetime.strptime(date_time, timestamp_format_minimal)

    if to_utc:
        return time.mktime(dt_obj.timetuple())

    else:
        return calendar.timegm(dt_obj.timetuple())


# utc time stamp -> utc date time string
def to_timestring(timestamp):
    date_time = datetime.datetime.utcfromtimestamp(timestamp)
    return date_time.strftime(timestamp_format_complete) + 'Z', date_time


parser = argparse.ArgumentParser(description=docstr)
parser.add_argument('-s', dest='start', type=check_valid_date,
                    help='starttime for the query period (YYYY-MM-DD[THH:MM:SS]). Defaults to start of current allocation period.')
parser.add_argument('-e', dest='end', type=check_valid_date,
                    help='endtime for the query period (YYYY-MM-DD[THH:MM:SS]). Defaults to NOW.',
                    default=datetime.datetime.utcnow().strftime(timestamp_format_complete))
parser.add_argument('-T', dest='MODE',
                    help='which target API to use', required=True,
                    choices=[MODE_MYBRC, MODE_MYLRC])
parser.add_argument('--PUSH', dest='push', action='store_true',
                    help='launch script in PROD mode, this will PUSH updates to the TARGET.')

parsed = parser.parse_args()
START = parsed.start
END = parsed.end
DEBUG = not parsed.push
MODE = parsed.MODE

PRICE_FILE = '/etc/slurm/bank-config.toml'
CONFIG_FILE = 'sync_running_jobs_{}.conf'.format(MODE)
LOG_FILE = ('sync_running_jobs_{}_debug.log' if DEBUG else 'sync_running_jobs_{}.log').format(MODE)
BASE_URL = 'https://{}/api/'.format('mybrc.brc.berkeley.edu' if MODE == MODE_MYBRC else 'mylrc.lbl.gov')

if START is None:
    current_month = datetime.datetime.now().month
    current_year = datetime.datetime.now().year
    break_month = 6 if MODE == MODE_MYBRC else 10
    year = current_year if current_month >= break_month else (current_year - 1)
    default_start = '{}-{}-01T00:00:00'.format(year, break_month)
    START = default_start

# convert to UTC
START = to_timestring(to_timestamp(START, to_utc=True))

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')

if not os.path.exists(CONFIG_FILE):
    print('config file {} missing'.format(CONFIG_FILE))
    logging.info('auth config file missing [{}], exiting run'.format(CONFIG_FILE))
    exit(0)

with open(CONFIG_FILE, 'r') as f:
    AUTH_TOKEN = f.read().strip()

if DEBUG:
    print('---DEBUG RUN---')

print('starting run, using endpoint {} START: {}'.format(BASE_URL, START))
logging.info('starting run, using endpoint {} START: {}'.format(BASE_URL, START))


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


def calculate_hours(duration_seconds):
    return duration_seconds / 3600


def calculate_amount(partition, cpu_count, duration_hrs):
    return round(get_price_per_hour(partition) * int(cpu_count) * duration_hrs, 2)


def calculate_cpu_time(num_cpus, duration_hrs):
    return duration_hrs * float(num_cpus)


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


def get_running_jobs():
    start_ts = to_timestamp(START)
    end_ts = to_timestamp(END)

    request_params = {'jobstatus': 'RUNNING',
                      'start_time': start_ts, 'end_time': end_ts}
    url_target = BASE_URL + 'jobs?' + urllib.urlencode(request_params)

    try:
        req = urllib2.Request(url_target)
        response = json.loads(urllib2.urlopen(req).read())
    except urllib2.URLError as e:
        if DEBUG:
            print('[get_running_jobs()] failed: {} {}'.format(request_params, e))
            logging.error('[get_running_jobs()] failed: {} {}'.format(request_params, e))

        return []

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
                print("\tgetting page: {}".format(current_page))

            if current_page > 50:
                print('too many jobs to update at once, rerun script after this run completes')
                logging.warning('too many jobs to update at once, rerun script after this run completes')
                break

        except urllib2.URLError as e:
            response['next'] = None

            if DEBUG:
                print('[get_running_jobs()] failed: {}'.format(e))
                logging.error('[get_running_jobs()] failed: {}'.format(e))

    return job_table


print('gathering running jobs from {}db'.format(MODE))
logging.info('gathering running jobs from {}db'.format(MODE))

# collect jobs
jobs = ''
for job in get_running_jobs():
    jobs += job['jobslurmid'] + '\n'

print('gathering latest state from slurmdb')
logging.info('gathering latest state from slurmdb')
lines = jobs.splitlines()

# collect job stats from slurm
jobids_str = ''
for line in lines:
    jobids_str += line.strip() + ','
jobids_str = jobids_str[:-1]

out = subprocess.Popen(['sacct', '-j', jobids_str,
                        '--format=JobId,Submit,Start,End,UID,Account,State,Partition,QOS,NodeList,AllocCPUS,ReqNodes,AllocNodes,CPUTimeRAW,CPUTime', '-n', '-P'],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
job_stats = out.communicate()[0].split_lines()

print('parsing jobs')
logging.info('parsing jobs')

# parse data
table = {}
for current in job_stats:
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

    try:
        submit, _ = to_timestring(to_timestamp(submit, to_utc=True))
        start, _start = to_timestring(to_timestamp(start, to_utc=True))
        end, _end = to_timestring(to_timestamp(end, to_utc=True))
        raw_time_hrs = calculate_hours((_end - _start).total_seconds())

        cpu_time = calculate_cpu_time(alloc_cpus, raw_time_hrs)
        amount = calculate_amount(partition, alloc_cpus, raw_time_hrs)
        node_list_converted = node_list_format(nodelist)

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
            'raw_time': raw_time_hrs,
            'cpu_time': float(cpu_time)}

    except Exception as e:
        logging.warning('ERROR occured for jobid: {} REASON: {}'.format(jobid, e))


if not DEBUG:
    print('updating mybrcdb with {} jobs'.format(len(table)))
    logging.info('updating mybrcdb with {} jobs'.format(len(table)))

else:
    print('DEBUG: collected {} jobs to update in mybrcdb'.format(len(table)))
    logging.info('DEBUG: collected {} jobs to update in mybrcdb'.format(len(table)))

    for jobid, job in table.items():
        logging.info('{} COLLECTED : {}'.format(jobid, job))

    print('DEBUG run complete, updated 0 jobs.')
    logging.info('DEBUG run complete, updated 0 jobs.')
    exit(0)

# push data
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
            print('\tprogress: {}/{}'.format(counter, len(table)))

    except urllib2.HTTPError as e:
        logging.warning('ERROR occured for jobid: {} REASON: {}'.format(jobid, e.reason))

print('run complete, updated {} jobs.'.format(counter))
logging.info('run complete, updated {} jobs.'.format(counter))
