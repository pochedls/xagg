#!/bin/bash

# example cron job for daily precip files

## activate environment
. /export_backup/pochedley1/bin/anaconda3/etc/profile.d/conda.sh
conda activate cdat81

## create log file name
NOW=$(date +"%F")
LOGFILE="pr_log-$NOW.log"

./xagg.py -p FALSE -f day -v pr,rlut,ua,hus,hur,wap,ta,zg,va,ps,rsdt,rsut,rsds,rsus,rlds,rlus,hfls,hfss > $LOGFILE

## migrate database
cd tools/
./migrateDatabase.sh
cd ..

## Cleanup
mv $LOGFILE logs/
cp xml.db backups/$NOW.db
gzip -f backups/$NOW.db