#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 19 August 2020

Dev script to remove xmls linked to retracted data.

@author: pochedls
"""

import os
import sqlite3
import sys
sys.path.append('..')
import fx
import datetime
import time

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
        xaggKeys[key]['path'] = [path]
        xaggKeys[key]['xmlfile'] = xmlfile
    else:
        xaggKeys[key]['path'] = xaggKeys[key]['path'] + [path]

# get paths that need to be retracted
print('Get paths to ignore')
print(time.ctime())
print()
retractList = []
deleteList = []
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
        retractList = retractList + xaggKeys[key]['path']
        fn = xaggKeys[key]['xmlfile']
        if fn is not None:
            deleteList = deleteList + [fn]

print('Retract files')
print(time.ctime())
print()
datalist = []
for rpath in retractList:
    ignoretime = fx.toSQLtime(datetime.datetime.now())
    datalist.append([None, None, 'retracted', 1, ignoretime, rpath])

columns = ['xmlFile', 'xmlwritedatetime', 'error', 'ignored', 'ignored_datetime']
fx.sqlUpdate(xaggDb, 'paths', columns, 'path', datalist)

print('Delete files')
print(time.ctime())
print()
deleteCount = 0
for xfn in deleteList:
    if xfn is None:
        continue
    if os.path.exists(xfn):
        os.remove(xfn)
        deleteCount += 1

print('Ignored ' + str(len(retractList)) + ' paths')
print('Removed ' + str(deleteCount) + ' xml files')
print(time.ctime())
print()

