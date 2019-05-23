#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

Script to compare paths with same metadata to see
if they contain the same files. 

Currently used to compare CMIP6 scratch versus publish information
for disagreements. 

@author: pochedls
"""

import sqlite3
import glob
import xdat 

sqlDB = '../xml.db'

q = "select p.path, p.keyId from paths p join (select keyId, path, count(*) as c from paths where mip_era = 'CMIP6' and retired = 0 group by keyId having count(*) > 1) innerQuery on innerQuery.keyId = p.keyId order by p.keyId;"

conn = sqlite3.connect(sqlDB) # connect to db
c = conn.cursor()
# get keys for which we have an xml file
c.execute(q)
result = c.fetchall()
conn.close() # 

def compareDirectories(files, files2):
	if len(files) != len(files2):
		return False
	for fn in files2:
		files2.remove(fn)
		files2.append(fn.split('/')[-1])
	for fn in files:
		files.remove(fn)
		files.append(fn.split('/')[-1])	
	for fn in files:
		x = xdat.findInList(fn, files2)
		if len(x) != 1:
			return False
	return True


bad = 0
pair1 = []
pair2 = []
for i in range(int(len(result)/2)):
	p1 = result[i*2][0]
	p2 = result[i*2+1][0]
	files = glob.glob(p1 + '*.nc')
	files2 = glob.glob(p2 + '*.nc')
	x = compareDirectories(files, files2)
	if not x:
		if p1.find('esgf_publish') > 0:
			print(bad, p1, p2)
		else:
			print(bad, p2, p1)
		pair1.append(p1)
		pair2.append(p2)
		bad += 1