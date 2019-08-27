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
mip_era = 'CMIP6'
testMode = False

files = glob.glob(base + 'wget/' + '*.nc')

for fn in files:
	version = '9'
	variable = fn.split('/')[-1].split('_')[0]
	rip = fn.split('/')[-1].split('_')[4]
	output = 'output0'	
	f = cdms2.open(fn)
	experiment = f.experiment_id
	frequency = f.frequency	
	if mip_era == 'CMIP5':
		institute = f.institute_id
		model = f.model_id
		realm = f.modeling_realm
		table = f.table_id.split('Table ')[1].split(' ')[0]
		fs = [base, mip_era, output, institute, model, experiment, frequency, realm, table, rip, version, variable]
	else:
		institute = f.institution_id
		model = f.source_id
		realm = f.realm
		activity = f.activity_id
		grid = f.grid_label
		table = f.table_id
		fs = [base, mip_era, activity, institute, model, experiment, rip, table, variable, grid, version]
	
	dirOut = '/'.join(fs) + '/'
	f.close()
	if testMode:
		print(fn, dirOut + fn.split('/')[-1])
	else:
		fx.ensure_dir(dirOut)
		os.rename(fn, dirOut + fn.split('/')[-1])
	
