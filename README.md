# MyBRC Slurm Banking Utilities

This repository contains utility scripts for various workflows involving the MyBRC
API (both the new coldfront version, and the old one) and the production SLURM
db. Refer to individual README.md files for details.

#### savio-check_usage

- contains the scripts called via the `check_usage.sh` command in production

#### generate-jobcomp-log

- contains scripts to generate the complete `jobcomp.log` file by collecting
  data from slurmdb

#### sync-brcdb

- contains scripts to clean up the inconsistencies MyBRC db and slurm db, which
  might happen due to downtime / outages
