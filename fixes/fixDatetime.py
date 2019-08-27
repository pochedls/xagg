#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

I had written the sqlite time out incorrectly. This fixed that. 

@author: pochedls
"""

import sys
sys.path.append('/export_backup/pochedley1/code/xagg/') 
import fx
import sqlite3
import datetime


# cp db.xml db-backup.xml

sqlDB = '../xml.db'

conn = sqlite3.connect(sqlDB)
c = conn.cursor()
# Create table
c.execute('select path, created, modified, accessed, xmlwritedatetime, retire_datetime, ignored_datetime from paths;')
data = c.fetchall()
conn.close() 


def fixTime(time):
	if not time:
		return time
	else:
		a = time.split(' ')[0]
		b = time.split(' ')[1]
		year = a.split('-')[0]
		month = "{:02d}".format(int(a.split('-')[1]))
		day = "{:02d}".format(int(a.split('-')[2]))
		hour = "{:02d}".format(int(b.split(':')[0]))
		minute = "{:02d}".format(int(b.split(':')[1]))
		second = "{:02d}".format(int(b.split(':')[2]))
		return year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second

dataOut = []
for row in data:
	fixRow = [fixTime(row[1]), fixTime(row[2]), fixTime(row[3]), fixTime(row[4]), fixTime(row[5]), fixTime(row[6]), row[0]]
	dataOut.append(fixRow)

columns = ['created', 'modified', 'accessed', 'xmlwritedatetime', 'retire_datetime', 'ignored_datetime']
constraint = 'path'
# dataOut
fx.sqlUpdate(sqlDB, 'paths', columns, constraint, dataOut)
