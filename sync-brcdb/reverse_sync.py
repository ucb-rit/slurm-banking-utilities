#!/usr/bin/python
import logging
import os
import urllib
import urllib2
import json
import subprocess

DEBUG = False
BASE_URL = 'https://mybrc.brc.berkeley.edu/api/'

LOG_FILE = 'reverse_sync.log'
CONFIG_FILE = 'reverse_sync.conf'

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')

if not os.path.exists(CONFIG_FILE):
    print('config file {} missing...'.format(CONFIG_FILE))
    logging.info('auth config file missing [{}], exiting run...'.format(CONFIG_FILE))
    exit()

with open(CONFIG_FILE, 'r') as f:
    AUTH_TOKEN = f.read().strip()

if DEBUG:
    print('---DEBUG RUN---')

print('starting run, using endpoint {} ...'.format(BASE_URL))
logging.info('starting run, using endpoint {} ...'.format(BASE_URL))


def paginate_requests(url, params=None):
    request_url = url
    params = params or {}

    if params:
        request_url = url + '?' + urllib.urlencode(params)

    try:
        req = urllib2.Request(request_url)
        response = json.loads(urllib2.urlopen(req).read())

    except urllib2.URLError as e:
        if DEBUG:
            print('[paginate_requests({}, {})] failed: {}'.format(url, params, e))
            logging.error('[paginate_requests({}, {})] failed: {}'.format(url, params, e))

        return []

    current_page = 0
    results = []
    results.extend(response['results'])
    while response['next'] is not None:
        try:
            current_page += 1
            params['page'] = current_page
            request_url = url + '?' + urllib.urlencode(params)
            req = urllib2.Request(request_url)
            response = json.loads(urllib2.urlopen(req).read())

            results.extend(response['results'])
            if current_page % 5 == 0:
                print('\tgetting page: {}'.format(current_page))

            if current_page > 50:
                print('too many pages to sync at once, rerun script after this run completes...')
                logging.warning('too many pages to sync at once, rerun script after this run completes...')
                break

        except urllib2.URLError as e:
            response['next'] = None

            if DEBUG:
                print('[paginate_requests()] failed: {}'.format(e))
                logging.error('[paginate_requests({}, {})] failed: {}'.format(url, params, e))

    return results


def single_request(url, params=None):
    request_url = url
    params = params or {}

    if params:
        request_url = url + '?' + urllib.urlencode(params)

    try:
        request = urllib2.Request(request_url)
        response = json.loads(urllib2.urlopen(request).read())
    except Exception as e:
        response = {'results': None}

        if DEBUG:
            print('[single_request({}, {})] failed: {}'.format(url, params, e))
            logging.error('[single_request({}, {})] failed: {}'.format(url, params, e))

    return response['results']


def get_project_allocation(project_name):
    allocation_id_url = BASE_URL + 'allocations/'
    response = single_request(allocation_id_url, {'project': project_name, 'resources': 'Savio Compute'})
    if not response or len(response) == 0:
        if DEBUG:
            print('[get_project_allocation({})] ERR'.format(project_name))
            logging.error('[get_project_allocation({})] ERR'.format(project_name))

        return None

    allocation_id = response[0]['id']
    allocation_url = allocation_id_url + '{}/attributes/'.format(allocation_id)
    response = single_request(allocation_url, {'type': 'Service Units'})
    if not response:
        return None

    allocation = response[0]['value']
    allocation = int(float(allocation))

    return allocation


def get_project_start(project_name):
    allocations_url = BASE_URL + 'allocations/'
    response = single_request(allocations_url, {'project': project_name, 'resources': 'Savio Compute'})
    if not response or len(response) == 0:
        if DEBUG:
            print('[get_project_start({})] ERR'.format(project_name))
            logging.error('[get_project_start({})] ERR'.format(project_name))

        return None

    creation = response[0]['start_date']
    if not creation:
        return None

    creation = creation if '.' not in creation else creation.split('.')[0]
    return '{}T00:00:00'.format(creation)


print('gathering accounts from mybrcdb...')
logging.info('gathering data from mybrcdb...')

project_table = paginate_requests(BASE_URL + 'projects/')
for project in project_table:
    project['allocation'] = get_project_allocation(project['name'])
    project['start'] = get_project_start(project['name'])

# NOTE: can use this to update fca.conf file
lines = []
for project in project_table:
    if not project['allocation'] or not project['start']:
        print('[project: {}] ERR, could not get allocation / start values'.format(project['name']))
        logging.error('[project: {}] ERR, could not get allocation / start values'.format(project['name']))
        continue

    lines.append('{}|{}|{}|Initial Allocation for {}'.format(
        project['name'], project['start'], project['allocation'], project['name']))

if DEBUG:
    print('debug run complete, exiting...')
    logging.info('debug run complete, exiting...')
    exit()

print('writing data to slurmdb...')
logging.info('writing data to slurmdb...')

commands = ''
for project in project_table:
    if ('allocation' not in project) or ('name' not in project):
        print('[project: {}] ERR, could not set allocation'.format(project['name']))
        logging.error('[project: {}] ERR, could not set allocation'.format(project['name']))
        continue

    # TODO: print commands to file
    command = 'sacctmgr modify account {} set GrpTRESMins="cpu={}"'.format(project['name'], project['allocation'])
    commands += '\n' + command

    # out, _ = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True).communicate()
    # print('updated account: {}, allocation set to: {}, with error: {}'.format(project['name'], project['allocation'], out))
    # logging.info('updated account: {}, allocation set to: {}, with error: {}'.format(project['name'], project['allocation'], out))

# print('updated allocation limits for {} accounts, run complete, exiting...'.format(len(project_table)))
# logging.info('updated allocation limits for {} accounts, run complete, exiting...'.format(len(project_table)))

with open('reverse_sync_output.sh', 'w') as f:
    f.writelines(commands)

print('run complete, wrote output to reverse_sync_output.sh, exiting...')
logging.info('run complete, wrote output to reverse_sync_output.sh, exiting...')

# sacctmgr modify account <account_name> set GrpTRESMins="cpu=xxxx"
