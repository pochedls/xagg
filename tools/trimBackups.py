#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 19 August 202

Helper script for trimming log output.

@author: pochedls
"""

import glob
import datetime
import numpy as np
from calendar import monthrange
import os

# directory
ldir = '/export_backup/pochedley1/code/xagg/backups/'
deleteFiles = True

# get files to process
files = glob.glob(ldir + '/*.db.gz')

# get range of months / years to iterate over
years = np.arange(2019, 2030)
months = np.arange(1, 13)

fileDict = {}
dateList = []
for fn in files:
    x = fn.split('/')[-1].split('.')[0].split('-')
    x = [int(x0) for x0 in x]
    dt = datetime.date(x[0], x[1], x[2])
    dateList.append(dt)
    fileDict[dt] = fn

dateList = np.sort(dateList)
keepList = []

# keep most recent backup
keepList.append(dateList[-1])
# keep last 30 days
for dt in dateList:
    if (datetime.date.today() - dt).days < 30:
        keepList.append(dt)

# get first file in each month
for year in years:
    for month in months:
        dtStart = datetime.date(year, month, 1)
        dtEnd = datetime.date(year, month, monthrange(year, month)[1])
        fsub = np.where((dateList >= dtStart) & (dateList <= dtEnd))[0]
        if len(fsub > 0):
            keepList.append(dateList[fsub[0]])

# get list of files to remove
rmList = []
for dt in dateList:
    if dt not in keepList:
        rmList.append(dt)

# process files
for dt in dateList:
    if dt in keepList:
        pre = '*'
    else:
        pre = ' '
        if deleteFiles:
            os.remove(fileDict[dt])
    print(pre + dt.isoformat() + ' ' + fileDict[dt])

