# Sync BRCDB Scripts

The purpose of these scripts is to remove the inconsistencies between SlurmDB
and MyBRC DB. These inconsistencies might happen due to downtime of the MyBRC
API or the Slurm Banking Plugins, and need to be patched up on regular basis to
avoid over/under charging users.

#### sync_brcdb.py

- **DEPRECATED**
- takes no arguments, and requires `filter_auth.conf` config file in same folder
  containing a MyBRC access token
- pulls Jobs left in `RUNNING` state in **OLD** MyBRC db and pushes the actual
  job complete times, SU usages, and other missing details to API by collecting
  live data from Slurm using `sacct` commands

#### update_jobs_coldfront.py

- takes optional arguments: start and end date; requires `update_jobs_coldfront.conf`
  config file in same folder, it will contain a MyBRC (coldfront) access token
- default start-end period is the current allocation, ie by default, only jobs
  in current allocation will get updated in MyBRC db
- pulls Jobs left in `RUNNING` state in MyBRC db and pushes the actual job
  complete times, SU usages, and other missing details to API by collecting
  live data from Slurm using `sacct` commands
- may need to run several times at first run (as it has a max limit of jobs it
  can update at one time)
- optionally takes **--debug** and **--target** flags to specify mode and target endpoint
  to use (default is production coldfront)

#### full_sync_coldfront.py

- takes optional arguments: start and end date; requires
  `full_sync_coldfront.conf` config file in same folder, it will contain a MyBRC
  (coldfront) access token
- only jobs in current allocation will get updated in MyBRC db, the start date
  is taken from MyBRC API **(this is not configurable)**. For projects which
  dont have a start date in MyBRC db like `vector_`, default start date is used,
  which is the last june 1
- pulls all **accounts/projects** from MyBRC db (coldfront), and collects all
  jobs in SLURM db belonging to each of those accounts. Pushes all of these
  collected jobs to the MyBRC db
- this will update jobs which have incorrect data (like start/end times,
  usages), and also push missing jobs into the MyBRC db
- may need to run several times at firs trun (as it has a max limit of jobs it
  can update/push at one time)
- optionally takes **--debug** and **--target** flags to specify mode and target endpoint
  to use (default is production coldfront)

**NOTE:**
this means that `full_sync_coldfront.py` is a superset of `update_jobs_coldfront.py`,
and the recommendation is to run the latter at frequent intervals, and the
former full sync less-frequently.

both the scripts produce comprehensive log files in the same folder, so their
actions (all jobs updated / pushed) can be monitored and changes can be
reverted if needed.
