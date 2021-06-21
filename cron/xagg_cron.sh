#!/bin/bash

# example cron job for standard run

## create log file name
NOW=$(date +"%F")
LOGFILE="log-$NOW.log"
exec &> $LOGFILE

## activate environment
. /home/pochedley1/bin/miniconda3/etc/profile.d/conda.sh
conda activate xagg

# run retract
./retract.py

# run xagg
./xagg.py # > $LOGFILE

## migrate database
cd tools/
./migrateDatabase.sh
cd ..
cp xml.db /p/user_pub/xclim/persist/xml.db

## Cleanup
mv $LOGFILE logs/
cp xml.db backups/$NOW.db
gzip -f backups/$NOW.db