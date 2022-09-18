# Sync Scripts

The purpose of these scripts is to remove the inconsistencies between SlurmDB
and MyBRC/MyLRC DB. These inconsistencies might happen due to downtime of the MyBRC/MyLRC
API or the Slurm Banking Plugins, and need to be patched up on regular basis to
avoid over/under charging users.

#### reverse_sync.py

**purpose:**

1. collects accounts from MyBRC API
2. outputs commands to update allocation values in SLURM
3. you can check then run these output commands to perform actual updates

**usage:**

```sh
$ python reverse_sync.py
```

**notes:**

- requires `reverse_sync.conf` file, which contains API token

#### sync_running_jobs.py

**purpose:**

1. collects `running` jobs from `TARGET` (MyBRC/MyLRC API)
2. collects updated stats about these jobs, from SLURM
3. pushes updated job stats to `TARGET` (MyBRC/MyLRC API)

**usage:**

```sh
$ python sync_running_jobs.py -T mybrc
```

**notes:**

- requires `sync_running_jobs_{mybrc/mylrc}.conf` files, which contain API token
- by default, it just collects and logs the changes it plans to make. To push
  actual changes to `TARGET`, look at `--PUSH` flag.
- default `-s` start is the current allocation period. (MyBRC: 06-01, MyLRC: 10-01)
- default `-e` end is current time (NOW)
- will overwrite data for jobs that already already exists in TARGET, with
  latest data
- generates `sync_running_jobs_{mybrc/mylrc}_{debug}.log` files for book keeping
- may need to run this multiple times, as it has a max limit of jobs it can
  update at one time. script will inform if this needs to be done

#### full_sync_coldfront.py

**purpose:**

1. collects accounts from TARGET (MyBRC/MyLRC API)
2. collects all jobs and their stats, for these accounts from SLURM
3. pushes all updates to `TARGET` (MyBRC/MyLRC API)

**usage:**

```sh
$ python full_sync_coldfront.py -T mybrc
```

**notes:**

- requires `full_sync_{mybrc/mylrc}.conf` files, which contain API token
- by default, it just collects and logs the changes it plans to make. To push
  actual changes to `TARGET`, look at `--PUSH` flag.
- collects jobs after start of project allocation (queried from TARGET)
- will overwrite data for jobs that already already exists in TARGET, with
  latest data
- generates `full_sync_{mybrc/mylrc}_{debug}.log` files for book keeping
- may need to run this multiple times, as it has a max limit of jobs it can
  update at one time. script will inform if this needs to be done

