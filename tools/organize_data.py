#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

Script (in development) to take data in an "extension" directory
and organize it adhering to normal CMIP path conventions so that it
can be scanned using xagg.

PJD 15 May 2020 - Update to extract mip_era from file global atts
PJD 17 Jun 2020 - Updated to use local fileArchive.mat
PJD 18 Jun 2020 - Updated to preserve file metadata (creation date)
PJD 18 Jun 2020 - Updated all xclim subdirs to xclimw group,
                  chmod 774 (rwxrwxr--)
                  Dirs updated: CMIP6, CMIP5, extension, persist

@author: pochedls
"""

import os
import sys
import glob
import scipy.io as sio
import cdms2
sys.path.append('..')
import fx

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
    version = '9'  # It is possible to generate from a file timestamp
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

    dirOut = os.path.join('', *fs)  # Splat operator deals with list type
    f.close()

    if testMode:
        print(fn, dirOut + fn.split('/')[-1])
    else:
        fx.ensure_dir(dirOut)
        # Copy file stats
        fileStat = os.stat(fn)
        print('file stat fn:', fn, fileStat.st_atime, fileStat.st_ctime, fileStat.st_mtime)
        fo = dirOut + fn.split('/')[-1]
        os.rename(fn, fo)
        # Apply stats back to new file
        os.utime(fo, (fileStat.st_atime, fileStat.st_mtime))
        fileStat = os.stat(fo)
        print('file stat fo:', fo, fileStat.st_atime, fileStat.st_ctime, fileStat.st_mtime)

if not testMode:
    sio.savemat('fileArchive.mat', {'fileArchive': fileArchive})
