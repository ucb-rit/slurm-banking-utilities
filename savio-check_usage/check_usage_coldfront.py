#!/usr/bin/python
import argparse
import datetime
import time
import getpass
import calendar

import urllib2
import urllib
import json


# staging is hit iff DEBUG is True
# production is hit iff DEBUG is False
DEBUG = False

VERSION = 2.0
docstr = '''
[version: {}]
'''.format(VERSION)

BASE_URL = 'http://scgup-dev.lbl.gov:8000/api/' if DEBUG else 'https://mybrc.brc.berkeley.edu/api/'
ALLOCATION_ENDPOINT = BASE_URL + 'allocations/'
ALLOCATION_USERS_ENDPOINT = BASE_URL + 'allocation_users/'
JOB_ENDPOINT = BASE_URL + 'jobs/'

timestamp_format_complete = '%Y-%m-%dT%H:%M:%S'
timestamp_format_minimal = '%Y-%m-%d'


def red_str(vector):
    return "\033[91m{}\033[00m".format(vector)


def green_str(vector):
    return "\033[92m{}\033[00m".format(vector)


def yellow_str(vector):
    return "\033[93m{}\033[00m".format(vector)


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

    if not complete and not minimal:  # doesn't fit either format
        raise argparse.ArgumentTypeError(
            'Invalid time specification {}'.format(s))
    else:
        return s


def to_timestamp(date_time):
    try:
        return calendar.timegm(time.strptime(date_time, timestamp_format_complete))
    except ValueError:
        return calendar.timegm(time.strptime(date_time, timestamp_format_minimal))


def to_timestring(timestamp):
    date_time = datetime.datetime.utcfromtimestamp(timestamp)
    return date_time.strftime(timestamp_format_complete)


def utc2local(utc):
    utc = datetime.datetime.utcfromtimestamp(utc)
    epoch = time.mktime(utc.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)

    local = utc + datetime.timedelta(hours=-7)  # + offset
    local = time.mktime(local.timetuple())  # calendar.timegm(local.timetuple())

    if DEBUG:
        print('[utc2local] utc_timestamp: {} local_timestamp: {}'.format(epoch, local))

    return local


def paginate_requests(url, params):
    request_url = url + '?' + urllib.urlencode(params)
    request = urllib2.Request(request_url)

    try:
        response = json.loads(urllib2.urlopen(request).read())
    except Exception, e:
        response = {'results': None}
        if DEBUG:
            print('[paginate_requests({}, {})] ERR: {}'.format(url, params, e))

        return []

    next_page = 2
    results = response['results']
    while response['next'] is not None:
        params['page'] = next_page
        request_url = url + '?' + urllib.urlencode(params)
        request = urllib2.Request(request_url)

        try:
            response = json.loads(urllib2.urlopen(request).read())
            results.extend(response['results'])
            next_page += 1
        except urllib2.URLError, e:
            response['next'] = None

            if DEBUG:
                print('[paginate_requests({}, {})] ERR: {}'.format(url, params, e))

    return results


def single_request(url, params=None):
    request_url = url
    if params:
        request_url += '?' + urllib.urlencode(params)
    request = urllib2.Request(request_url)

    try:
        response = json.loads(urllib2.urlopen(request).read())
    except Exception, e:
        response = {'results': None}

        if DEBUG:
            print('[single_request({}, {})] ERR: {}'.format(url, params, e))

    return response['results']


def get_project_start(project):
    allocation_id_url = ALLOCATION_ENDPOINT
    params = {'project': project, 'resources': 'Savio Compute'}

    response = single_request(allocation_id_url, params)
    if not response or len(response) == 0:
        if DEBUG:
            print('[get_project_start({}, {})] ERR'.format(project, user))

        return None

    creation = response[0]['start_date']
    return creation.split('.')[0] if '.' in creation else creation


current_month = datetime.datetime.now().month
current_year = datetime.datetime.now().year
default_start = '{}-06-01T00:00:00'.format(current_year if current_month >= 6 else (current_year - 1))

parser = argparse.ArgumentParser(description=docstr)
parser.add_argument('-u', dest='user',
                    help='check usage of this user')
parser.add_argument('-a', dest='account',
                    help='check usage of this account')

parser.add_argument('-E', dest='expand', action='store_true',
                    help='expand user/account usage')
parser.add_argument('-s', dest='start', type=check_valid_date,
                    help='starttime for the query period (YYYY-MM-DD[THH:MM:SS])',
                    default=default_start)
parser.add_argument('-e', dest='end', type=check_valid_date,
                    help='endtime for the query period (YYYY-MM-DD[THH:MM:SS])',
                    default=datetime.datetime.now().strftime(timestamp_format_complete))
parsed = parser.parse_args()
user = parsed.user
account = parsed.account
expand = parsed.expand
_start = parsed.start
_end = parsed.end
start = to_timestamp(_start)
end = to_timestamp(_end)

default_start_used = _start == default_start
calculate_project_start = default_start_used and account

if calculate_project_start:
    target_start_date = get_project_start(account)
    if target_start_date is not None:
        _start = to_timestring(to_timestamp(target_start_date))
        start = to_timestamp(_start)
    elif DEBUG:
        print('[get_account_start({})] ERR'.format(account))

# defaults
if not user and not account:
    user = getpass.getuser()

output_headers = {}
if user:
    output_header = 'Usage for USER {} [{}, {}]:'.format(user, _start, _end)
    output_headers['user'] = output_header

if account:
    output_header = 'Usage for ACCOUNT {} [{}, {}]:'.format(account, _start, _end)
    output_headers['account'] = output_header


def get_cpu_usage(user=None, account=None):
    params = {'start_time': start, 'end_time': end}
    if user:
        params['user'] = user

    if account:
        params['account'] = account

    request_url = JOB_ENDPOINT + '?' + urllib.urlencode(params)
    request = urllib2.Request(request_url)

    try:
        response = json.loads(urllib2.urlopen(request).read())
    except Exception, e:
        response = {'count': 0, 'total_cpu_time': 0, 'total_amount': 0,
                    'response': [], 'next': None}

        if DEBUG:
            print('[get_cpu_usage({}, {})] ERR: {}'.format(user, account, e))

        if user and not account:
            return -1, -1, -1

    job_count = response['count']
    total_cpu = response['total_cpu_time']
    total_amount = response['total_amount']

    return job_count, total_cpu, total_amount


def process_account_query():
    allocation_id_url = ALLOCATION_ENDPOINT
    response = single_request(allocation_id_url, {'project': account, 'resources': 'Savio Compute'})
    if not response or len(response) == 0:
        if DEBUG:
            print('[process_account_query()] ERR')

        print('ERR: Account not found: {}'.format(account))
        return

    allocation_id = response[0]['id']
    allocation_url = allocation_id_url + '{}/attributes/'.format(allocation_id)
    response = single_request(allocation_url, {'type': 'Service Units'})
    if not response or len(response) == 0:
        if DEBUG:
            print('[process_account_query()] ERR')

        raise urllib2.URLError('Backend Error, contact BRC Support (brc-hpc-help@berkeley.edu).')

    allocation = response[0]['value']
    allocation_attribute_id = response[0]['id']
    allocation = int(float(allocation))

    if 'ac_' in account or 'co_' in account:
        # get usage from history
        account_usage_url = allocation_url + '{}/history/'.format(allocation_attribute_id)
        response = single_request(account_usage_url, None)
        if not response or len(response) == 0:
            if DEBUG:
                print('[process_account_query()] ERR')

            raise urllib2.URLError('Backend Error, contact BRC Support (brc-hpc-help@berkeley.edu).')

        account_usage = response[0]['value']
        job_count, cpu_usage = 0, 0.0
    else:
        # get usage from jobs
        job_count, cpu_usage, account_usage = get_cpu_usage(account=account)

    if not default_start_used:
        print output_headers['account'], job_count, 'jobs,', '{:.2f}'.format(cpu_usage), 'CPUHrs,', account_usage, 'SUs.'
    else:
        print output_headers['account'], job_count, 'jobs,', '{:.2f}'.format(cpu_usage), 'CPUHrs,', account_usage, 'SUs used from an allocation of', allocation, 'SUs.'

    if expand:
        user_url = ALLOCATION_USERS_ENDPOINT
        user_list = paginate_requests(user_url, {'project': account})

        for user in user_list:
            if user['user'] is None:
                continue

            user_name = user['user']
            user_jobs, user_cpu, user_usage = get_cpu_usage(user_name, account)

            percentage = 0.0
            try:
                percentage = (float(user_usage) / float(account_usage)) * 100
            except Exception:
                percentage = 0.00

            if percentage < 75:
                color_fn = green_str
            elif percentage > 100:
                color_fn = red_str
            else:
                color_fn = yellow_str

            percentage = color_fn("{:.2f}".format(percentage))
            print('\tUsage for USER {} in ACCOUNT {} [{}, {}]: {} jobs, {:.2f} CPUHrs, {} ({}%) SUs.'
                  .format(user_name, account, _start, _end, user_jobs, user_cpu, user_usage, percentage))


def process_user_query():
    global start, _start

    # use default start
    if default_start_used:
        _start = default_start
        start = to_timestamp(default_start)

    total_jobs, total_cpu, total_usage = get_cpu_usage(user)
    if total_jobs == total_cpu == total_usage == -1:
        print('ERR: User not found: {}'.format(user))
        return

    print output_headers['user'], total_jobs, 'jobs,', '{:.2f}'.format(total_cpu), 'CPUHrs,', total_usage, 'SUs used.'

    if expand:
        user_allocation_url = ALLOCATION_USERS_ENDPOINT
        response = paginate_requests(user_allocation_url, {'user': user})

        for allocation in response:
            allocation_account = allocation['project']
            allocation_jobs, allocation_cpu, allocation_usage = get_cpu_usage(user, allocation_account)

            print('\tUsage for USER {} in ACCOUNT {} [{}, {}]: {} jobs, {:.2f} CPUHrs, {} SUs.'
                  .format(user, allocation_account, _start, _end, allocation_jobs, allocation_cpu, allocation_usage))


for req_type in output_headers.keys():
    try:
        if start > end:
            print('ERROR: Start time ({}) requested is after end time ({}).'.format(_start, _end))
            exit(0)

        if to_timestamp('2020-06-01') > start:
            print('INFO: Information might be inaccurate, for accurate information contact BRC support (brc-hpc-help@berkeley.edu).')

        if req_type == 'user':
            process_user_query()

        if req_type == 'account':
            if account.startswith('ac_'):
                print('INFO: Start Date shown may be inaccurate.')

            process_account_query()

    except urllib2.URLError, e:
        print('ERROR: Could not connect to backend, contact BRC Support (brc-hpc-help@berkeley.edu) if problem persists.')
        if DEBUG:
            print('__main__ ERR: {}'.format(e))

    except Exception, e:
        if DEBUG:
            print('__main__ ERR: {}'.format(e))
