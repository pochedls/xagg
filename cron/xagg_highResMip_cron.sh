#!/bin/bash

# example cron job for standard run

## create log file name
NOW=$(date +"%F")
LOGFILE="hrm_log-$NOW.log"
exec &> $LOGFILE

## activate environment
. /export_backup/pochedley1/bin/anaconda3/etc/profile.d/conda.sh
conda activate xagg

./xagg.py -p FALSE -e control-1950,highres-future,highresSST-future,highresSST-present,hist-1950,spinup-1950 -f 1hr,3hr,6hr,day -v pr > $LOGFILE

## migrate database
cd tools/
./migrateDatabase.sh
cd ..

## Cleanup
mv $LOGFILE logs/
cp xml.db backups/$NOW.db
gzip -f backups/$NOW.db