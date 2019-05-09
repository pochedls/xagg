#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

This code will compare xmls actually on disk to those 
in the database and generate a list of xml files missing on 
disk (with optional functionality to remove them from the
database). 

@author: pochedls
"""

import os
import sqlite3
import numpy as np

removeFromDB = False # if true, paths that are not on disk will be removed from the database

sqlDB = 'xml.db'

conn = sqlite3.connect(sqlDB)
c = conn.cursor()
# Create table
c.execute('select xmlFile from paths where xmlFile is not null;')
a = c.fetchall()
conn.close() 

missing = []
for i, row in enumerate(a):
	if np.mod(i, 1000) == 0:
		print(str(i) + ' / ' + str(len(a)))
	fn = row[0]
	if not os.path.exists(fn):
		missing.append(fn)


if removeFromDB:
	ps = '\'' + '\', \''.join(missing) + '\''
	q = 'UPDATE paths SET xmlFile = NULL, xmlwritedatetime = NULL, error = NULL WHERE xmlFile in (' + ps + ');'
	conn = sqlite3.connect(sqlDB)
	c = conn.cursor()
	c.execute(q) 
	conn.commit()
	conn.close()


