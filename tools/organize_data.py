#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

Script (in development) to take data in a "extension" directory
and organize it adhering to normal CMIP path conventions so that it 
can be scanned using xagg. 

Only works for CMIP5 right now. 

@author: pochedls
"""

import glob
import cdms2
import os
import sys
sys.path.append('..')
import fx

base = '/p/user_pub/xclim/extension/'
mip_era = 'CMIP5'
testMode = False

files = glob.glob(base + 'wget/' + '*.nc')

for fn in files:
	f = cdms2.open(fn)
	institute = f.institute_id
	model = f.model_id
	experiment = f.experiment_id
	frequency = f.frequency
	realm = f.modeling_realm
	table = f.table_id.split('Table ')[1].split(' ')[0]
	version = '9'
	variable = fn.split('/')[-1].split('_')[0]
	rip = fn.split('/')[-1].split('_')[4]
	output = 'output0'
	fs = [base, mip_era, output, institute, model, experiment, frequency, realm, table, rip, version, variable]
	dirOut = '/'.join(fs) + '/'
	f.close()
	fx.ensure_dir(dirOut)
	if testMode:
		print(fn, dirOut + fn.split('/')[-1])
	else:
		os.rename(fn, dirOut + fn.split('/')[-1])
	
