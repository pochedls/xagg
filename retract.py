#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 19 August 2020

Script to remove xmls linked to retracted data.

@author: pochedls
"""

import os
import sqlite3
import fx
import datetime
import time
import glob


# define convenience function
def execQuery(sqlDb, q):
    conn = sqlite3.connect(sqlDb)
    c = conn.cursor()
    cursor = c.execute(q)
    x = cursor.fetchall()
    names = list(map(lambda x: x[0], cursor.description))
    conn.close()
    return names, x


# specify databases
sqlDb = '/p/css03/painter/db/sdt6.db'
xaggDb = 'xml.db'
cmipMeta = 'data/cmipMeta.pkl'
retractDir = '/p/user_pub/xclim/retracted/'
testDir = '/p/user_pub/xclim/CMIP6/CMIP/amip/atmos/mon/tas/'

# check if xagg is mounted
files = glob.glob(testDir + '*.xml')
if len(files) < 100:
    raise ValueError('It appears a disk is not mounted.')

# get retracted files
print('Get retracted files')
print(time.ctime())
print()
q = "SELECT * FROM dataset WHERE status LIKE '%retracted';"
names, retractedSet = execQuery(sqlDb, q)

# get all paths, keys, and xmls
print('Get xagg files')
print(time.ctime())
print()
q = 'select keyid, path, xmlfile from paths where retired = 0 and ignored = 0;'
x, allKeys = execQuery(xaggDb, q)
xaggKeys = {}
for row in allKeys:
    key = row[0]
    path = row[1]
    xmlfile = row[2]
    if key not in xaggKeys.keys():
        xaggKeys[key] = {}
        xaggKeys[key][path] = xmlfile
    else:
        xaggKeys[key][path] = xmlfile

# get paths that need to be retracted
print('Get paths to ignore')
print(time.ctime())
print()
retractList = []
deleteList = {}
datalist = []
datalistXml = []
for i, row in enumerate(retractedSet):
    # get metadata
    meta = row[1].split('.')
    mip = meta[0]
    activity = meta[1]
    institution = meta[2]
    model = meta[3]
    experiment = meta[4]
    realization = meta[5]
    table = meta[6]
    variableId = meta[7]
    grid = meta[8]
    version = meta[9]
    gridLabel = '*'
    try:
        frequency, realm, dimensions = fx.lookupCMIPMetadata(mip, table,
                                                             variableId,
                                                             dictObj=cmipMeta)
        gridLabel = fx.createGridLabel(mip, realm, table,
                                       grid, dimensions)
    except:
        realm = 'unk'
        frequency = 'unk'
        gridLabel = 'unk'

    # create key
    key = [mip, activity, institution, model, experiment, realization, table,
           realm, frequency, variableId, grid, gridLabel, version]
    key = '.'.join(key)
    if key in xaggKeys.keys():
        for rpath in xaggKeys[key].keys():
            fn = xaggKeys[key][rpath]
            ignoretime = fx.toSQLtime(datetime.datetime.now())
            if fn is None:
                datalist.append([None, None, 'retracted', 1, ignoretime, rpath])
            else:
                xfnn = retractDir + fn.split('/')[-1]
                deleteList[fn] = xfnn
                datalistXml.append([xfnn, 'retracted', 1, ignoretime, rpath])


print('Retract files')
print(time.ctime())
print()

# Ignore paths without an xml file
columns = ['xmlFile', 'xmlwritedatetime', 'error', 'ignored', 'ignored_datetime']
fx.sqlUpdate(xaggDb, 'paths', columns, 'path', datalist)

# Update paths with an xml file
columnsXml = ['xmlFile', 'error', 'ignored', 'ignored_datetime']
fx.sqlUpdate(xaggDb, 'paths', columnsXml, 'path', datalistXml)

print('Archive files')
print(time.ctime())
print()
deleteCount = 0
for fn in deleteList:
    if os.path.exists(fn):
        xfnn = deleteList[fn]
        os.rename(fn, xfnn)
        deleteCount += 1

print('Ignored ' + str(len(datalist)) + ' paths')
print('Archived ' + str(deleteCount) + ' xml files')
print(time.ctime())
print()
