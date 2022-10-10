#!/usr/bin/python
import logging
import os
import urllib
import urllib2
import json
import argparse
import subprocess


docstr = '''
Sync projects (and all their jobs) between MyBRC/MyLRC with Slurm-DB.
This will update SLURM with the latest values for each job collected from MyBRC/MyLRC.

This will only write out a file containing all changes to be made (corresponding slurm commands).
It will not update ANY data in SLURM on its own. To do so, run the output file generated.
'''

MODE_MYBRC = 'mybrc'
MODE_MYLRC = 'mylrc'

parser = argparse.ArgumentParser(description=docstr)
parser.add_argument('-T', dest='MODE',
                    help='which target API to use', required=True,
                    choices=[MODE_MYBRC, MODE_MYLRC])

parsed = parser.parse_args()
MODE = parsed.MODE
DEBUG = False

CONFIG_FILE = 'reverse_sync_{}.conf'.format(MODE)
LOG_FILE = ('reverse_sync_{}_debug.log' if DEBUG else 'reverse_sync_{}.log').format(MODE)
BASE_URL = 'https://{}/api/'.format('mybrc.brc.berkeley.edu' if MODE == MODE_MYBRC else 'mylrc.lbl.gov')

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')

if not os.path.exists(CONFIG_FILE):
    print('config file {0} missing...'.format(CONFIG_FILE))
    logging.info('auth config file missing [{0}], exiting run...'.format(CONFIG_FILE))
    exit()

with open(CONFIG_FILE, 'r') as f:
    AUTH_TOKEN = f.read().strip()

if DEBUG:
    print('---DEBUG RUN---')

print('starting run, using endpoint {0} ...'.format(BASE_URL))
logging.info('starting run, using endpoint {0} ...'.format(BASE_URL))


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
            print('[paginate_requests({0}, {1})] failed: {2}'.format(url, params, e))
            logging.error('[paginate_requests({0}, {1})] failed: {2}'.format(url, params, e))

        return []

    current_page = 1
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
                print('\tgetting page: {0}'.format(current_page))

            if current_page > 50:
                print('too many pages to sync at once, rerun script after this run completes...')
                logging.warning('too many pages to sync at once, rerun script after this run completes...')
                break

        except urllib2.URLError as e:
            response['next'] = None

            if DEBUG:
                print('[paginate_requests()] failed: {0}'.format(e))
                logging.error('[paginate_requests({0}, {1})] failed: {2}'.format(url, params, e))

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
            print('[single_request({0}, {1})] failed: {2}'.format(url, params, e))
            logging.error('[single_request({0}, {1})] failed: {2}'.format(url, params, e))

    return response['results']


def get_project_allocation(project_name):
    allocation_id_url = BASE_URL + 'allocations/'
    compute_resources = '{} Compute'.format('Savio' if MODE == MODE_MYBRC else 'LAWRENCIUM')
    response = single_request(allocation_id_url, {'project': project_name, 'resources': compute_resources})
    if not response or len(response) == 0:
        if DEBUG:
            print('[get_project_allocation({0})] ERR'.format(project_name))
            logging.error('[get_project_allocation({0})] ERR'.format(project_name))

        return None

    allocation_id = response[0]['id']
    allocation_url = allocation_id_url + '{0}/attributes/'.format(allocation_id)
    response = single_request(allocation_url, {'type': 'Service Units'})
    if not response:
        return None

    allocation = response[0]['value']
    allocation = int(float(allocation))

    return allocation


def get_project_start(project_name):
    allocations_url = BASE_URL + 'allocations/'
    compute_resources = '{} Compute'.format('Savio' if MODE == MODE_MYBRC else 'LAWRENCIUM')
    response = single_request(allocations_url, {'project': project_name, 'resources': compute_resources})
    if not response or len(response) == 0:
        if DEBUG:
            print('[get_project_start({0})] ERR'.format(project_name))
            logging.error('[get_project_start({0})] ERR'.format(project_name))

        return None

    creation = response[0]['start_date']
    if not creation:
        return None

    return creation if '.' not in creation else creation.split('.')[0]
    # return '{0}T00:00:00'.format(creation)


print('gathering accounts from {}db...'.format(MODE))
logging.info('gathering data from {}db...'.format(MODE))

# NOTE(vir): ignore abc and vector for now
project_table = paginate_requests(BASE_URL + 'projects/')
project_table = filter(
    lambda p: p['name'] != 'abc' and not p['name'].startswith('vector_'),
    project_table)
for project in project_table:
    project['allocation'] = get_project_allocation(project['name'])
    project['start'] = get_project_start(project['name'])

# NOTE(vir): can use this to update fca.conf file
'''
lines = []
for project in project_table:
    if ('allocation' not in project) or ('name' not in project) or (project['allocation'] == None):
        print('[project: {}] ERR, could not set allocation (value={})'.format(project['name'], project['allocation']))
        logging.error('[project: {}] ERR, could not set allocation (value={})'.format(project['name'], project['allocation']))
        continue

    lines.append('{}|{}|{}|Initial Allocation for {}'.format(
        project['name'], project['start'], project['allocation'], project['name']))
'''

print('writing data to file (slurmdb commands)...')
logging.info('writing data to file (slurmdb commands)...')

commands = ''
for project in project_table:
    if ('allocation' not in project) or ('name' not in project) or (project['allocation'] == None):
        print('[project: {0}] ERR, could not set allocation (value={1})'.format(project['name'], project['allocation']))
        logging.error('[project: {0}] ERR, could not set allocation (value={1})'.format(project['name'], project['allocation']))
        continue

    allocation_in_seconds = 60 * project['allocation']
    command = 'yes | sacctmgr modify account {0} set GrpTRESMins="cpu={1}"'.format(project['name'], allocation_in_seconds)
    commands += '\n' + command

    # NOTE(vir): actually update data in SLURM
    # out, _ = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True).communicate()
    # print('updated account: {}, allocation set to: {}, with error: {}'.format(project['name'], project['allocation'], out))
    # logging.info('updated account: {}, allocation set to: {}, with error: {}'.format(project['name'], project['allocation'], out))

# print('updated allocation limits for {} accounts, run complete, exiting...'.format(len(project_table)))
# logging.info('updated allocation limits for {} accounts, run complete, exiting...'.format(len(project_table)))

with open('reverse_sync_output_{}.sh'.format(MODE), 'w') as f:
    f.writelines(commands)

print('run complete, wrote output to reverse_sync_output_{}.sh, exiting...'.format(MODE))
logging.info('run complete, wrote output to reverse_sync_output_{}.sh, exiting...'.format(MODE))

# sacctmgr modify account <account_name> set GrpTRESMins="cpu=xxxx"
