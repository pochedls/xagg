#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Stephen Po-Chedley 9 May 2019

Script (in development) to take data in an "extension" directory
and organize it adhering to normal CMIP path conventions so that it
can be scanned using xagg.

PJD 15 May 2020 - Update to extract mip_era from file global atts
PJD 17 Jun 2020 - Updated to use local fileArchive.mat

@author: pochedls
"""

import os
import sys
import glob
import scipy.io as sio
import fx
import cdms2

sys.path.append('..')


base = '/p/user_pub/xclim/extension/'
testMode = False
deleteRepeats = True
overCopyRepeats = False

# load existing files into list
fa = sio.loadmat(os.path.join(base, 'fileArchive.mat'))['fileArchive']
fileArchive = [fn.replace(' ', '') for fn in fa]

files = glob.glob(base + 'wget/' + '*.nc')

for fn in files:
    if fn.split('/')[-1] in fileArchive:
        print(fn + ' already archived...')
        if deleteRepeats:
            os.remove(fn)
        if overCopyRepeats:
            pass
        else:
            continue
    else:
        fileArchive.append(fn.split('/')[-1])
    version = '9'
    variable = fn.split('/')[-1].split('_')[0]
    rip = fn.split('/')[-1].split('_')[4]
    output = 'output0'
    f = cdms2.open(fn)
    experiment = f.experiment_id
    frequency = f.frequency
    if 'mip_era' in f.attributes.keys():
        # Case CMIP6
        mip_era = f.mip_era
        institute = f.institution_id
        model = f.source_id
        realm = f.realm
        activity = f.activity_id
        grid = f.grid_label
        table = f.table_id
        fs = [base, mip_era, activity, institute, model, experiment, rip,
              table, variable, grid, version]
    else:
        # Case CMIP5 (or CMIP3)
        mip_era = 'CMIP5'
        institute = f.institute_id
        model = f.model_id
        realm = f.modeling_realm
        table = f.table_id.split('Table ')[1].split(' ')[0]
        fs = [base, mip_era, output, institute, model, experiment, frequency,
              realm, table, rip, version, variable]

    dirOut = '/'.join(fs) + '/'
    f.close()
    if testMode:
        print(fn, dirOut + fn.split('/')[-1])
    else:
        fx.ensure_dir(dirOut)
        os.rename(fn, dirOut + fn.split('/')[-1])

if not testMode:
    sio.savemat('fileArchive.mat', {'fileArchive': fileArchive})
