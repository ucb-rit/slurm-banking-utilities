#!/bin/bash

# Generate a CSV containing the Slurm GrpTRESMins value and equivalent
# number of service units for each account starting with the given
# prefix.

# Check if the ACCOUNT_PREFIX argument is provided.
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <ACCOUNT_PREFIX>"
    exit 1
fi

ACCOUNT_PREFIX=$1

# Print the CSV header.
echo "Account,GrpTRESMins CPU,Service Units"

# Get all unique account names starting with the provided prefix.
account_names=$(sacctmgr show account -s format=account --noheader --parsable2 | grep -E "^$ACCOUNT_PREFIX" | sort -u)

# Check if account_names is empty.
if [ -z "$account_names" ]; then
    echo "No accounts found starting with the prefix: $ACCOUNT_PREFIX"
    exit 1
fi

# Loop through each unique matching account name.
for ACCOUNT_NAME in $account_names; do
    # Get the GrpTRESMins value for the specified account.
    cpu_values=$(sacctmgr show association where account="$ACCOUNT_NAME" where User="" format=GrpTRESMins --noheader --parsable2 | \
                 cut -d'=' -f2)

    # Process each CPU value.
    while read -r cpu; do
        # Only process if CPU has a value.
        if [ -n "$cpu" ]; then
            # Calculate the equivalent number of service units.
            num_service_units=$(echo "$cpu / 60" | bc)

            # Print the results as CSV.
            echo "$ACCOUNT_NAME,$cpu,$num_service_units"
        fi
    done <<< "$cpu_values"  # Read from the cpu_values variable.
done
