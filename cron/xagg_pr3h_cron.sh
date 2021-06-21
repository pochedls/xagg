#!/bin/bash

# example cron job for daily precip files

## activate environment
. /home/pochedley1/bin/miniconda3/etc/profile.d/conda.sh
conda activate xagg

## create log file name
NOW=$(date +"%F")
LOGFILE="pr3h_log-$NOW.log"

./xagg.py -p FALSE -f 3hr -v pr,prw -e amip,historical > $LOGFILE

## migrate database
cd tools/
./migrateDatabase.sh
cd ..

## Cleanup
mv $LOGFILE logs/
cp xml.db backups/$NOW.db
gzip -f backups/$NOW.db
