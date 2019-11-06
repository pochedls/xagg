#!/bin/bash

# example cron job for standard run

## activate environment
. /export_backup/pochedley1/bin/anaconda3/etc/profile.d/conda.sh
conda activate xagg

## create log file name
NOW=$(date +"%F")
LOGFILE="log-$NOW.log"

./xagg.py > $LOGFILE

## migrate database
cd tools/
./migrateDatabase.sh
cd ..

## Cleanup
mv $LOGFILE logs/
cp xml.db backups/$NOW.db
gzip -f backups/$NOW.db