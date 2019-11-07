#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

This is a wrapper for runing xagg software. It makes use of:
    runSettings.py  - runtime variables for software
    fx.py           - functions called by this wrapper

There are a number of command line options. These can be viewed using:
    ./xagg.py --help

The environment was created / implemented using:

    conda create -n cdat81 -c cdat/label/v81 -c conda-forge cdat scandir joblib
    conda activate cdat81

@author: pochedls
"""

import fx
import time
import os
from runSettings import *
import numpy as np
import argparse
try:
    __IPYTHON__
except NameError:
    INIPYTHON = False
else:
    INIPYTHON = True


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if INIPYTHON is False:  # Look for cmd line arguments if we are NOT in Ipython

    parser = argparse.ArgumentParser()

    # Optional arguments
    parser.add_argument('-p', '--updatePaths', type=str2bool,
                        default=True,
                        help="Flag (TRUE/FALSE) to update SQL database" +
                             " (default is TRUE)")
    parser.add_argument('-s', '--updateScans', type=str2bool,
                        default=True,
                        help="Flag (TRUE/FALSE) to run cdscan" +
                             " (default is TRUE)")
    parser.add_argument('-out', '--outputDirectory', type=str,
                        default='/p/user_pub/xclim/',
                        help="Base output directory for xml files" +
                             " (default /p/user_pub/xclim/)")
    parser.add_argument('-n', '--numProcessors', type=int,
                        default=20,
                        help="Number of processors for creating xml files" +
                             " (default 20)")
    parser.add_argument('-c', '--countStats', type=str2bool,
                        default=True,
                        help="Boolean to record statistics on xml database")
    parser.add_argument('-e', '--experiment', type=str,
                        default='',
                        help="Comma separated list of experiments" +
                             " (e.g., piControl,abrupt4xCO2,rcp85)")
    parser.add_argument('-f', '--frequency', type=str,
                        default='',
                        help="Comma separated list of frequencies" +
                             " (e.g., mon,day,3hr)")
    parser.add_argument('-v', '--variable', type=str,
                        default='',
                        help="Comma separated list of variables" +
                             " (e.g., tas,ts,ps)")

    args = parser.parse_args()

    updatePaths = args.updatePaths
    updateScans = args.updateScans
    outputDirectory = args.outputDirectory
    numProcessors = args.numProcessors
    countStats = args.countStats
    experimentIn = args.experiment
    variableIn = args.variable
    frequencyIn = args.frequency

else:

    updatePaths = True
    updateScans = True
    outputDirectory = '/p/user_pub/xclim/'
    numProcessors = 20
    countStats = True
    experimentIn = ''
    variableIn = ''
    frequencyIn = ''

print('Starting job')
print(time.ctime())
print()

# Ensure there isn't a concurrent run or unresolved error
# If there is no lock, place a lock and continue
if fx.runLock('check'):
    raise ValueError('Lock is on. xagg is running or encountered an error.')
else:
    fx.runLock('on')

# override default variables with user inputs
if experimentIn != '':
    experiments = experimentIn.split(',')

if frequencyIn != '':
    frequencies = frequencyIn.split(',')

if variableIn != '':
    variables = variableIn.split(',')

# ensure database is initialized
if not os.path.exists(sqlDB):
    print('Initializing database')
    print(time.ctime())
    print()
    fx.initializeDB(sqlDB)

# get all paths
if updatePaths:
    print('Checking disk paths')
    print(time.ctime())
    print()
    diskStat = fx.parallelFindData(data_directories, split=split_directories,
                                   rmDir=rm_directories)
    diskPaths = diskStat.keys()

# ensure there is a cmip metadata file
if not os.path.exists(cmipMetaFile):
    print('Initializing CMIP Meta File')
    print(time.ctime())
    print()
    fx.createLookupDictionary(diskPaths, outfile=cmipMetaFile)

# get paths in database
print()
print('Get existing xml metadata')
print(time.ctime())
print()
db = fx.getDBPaths(sqlDB)
dbPaths = db.keys()
invalidPaths = fx.getInvalidDBPaths(sqlDB)
retiredPaths = fx.getRetiredDBPaths(sqlDB)

# compare paths on disk to those in database
if updatePaths:
    print('Comparing disk paths with database')
    print(time.ctime())
    print()
    fx.updateDatabaseHoldings(sqlDB, diskPaths, diskStat, dbPaths, db,
                              invalidPaths, retiredPaths)

# get paths to scan
if updateScans:
    print('Getting paths to scan')
    print(time.ctime())
    print()
    db = fx.getDBPaths(sqlDB)  # get updated database
    scanList = fx.getScanList(sqlDB, db, variables, experiments, frequencies)

    print('Start scans')
    print(time.ctime())
    print()

    nChunks = int(np.ceil(len(scanList)/chunkSize))
    nTotal = 0
    for i in range(nChunks):
        # get a chunk of the scanList
        Idx = np.arange(i * chunkSize, i*chunkSize + chunkSize)
        if Idx[-1] > len(scanList):
            Idx = np.arange(i * chunkSize, len(scanList))
        inList = [scanList[i] for i in Idx]
        nTotal += len(inList)
        print(time.ctime() + ': ' + str(nTotal) + '/' + str(len(scanList)) +
              ' (' + str(np.round(nTotal/len(scanList)*100, 1)) + '%) ',
              end='')
        # scan chunk
        s = time.time()
        results = fx.scanChunk(inList, numProcessors, outputDirectory)
        e = time.time()
        print('- ' + str(int(e-s)) + 's ', end='')
        # write results to database
        s = time.time()
        fx.writeScanResults(sqlDB, results)
        e = time.time()
        print('- ' + str(int(e-s)) + 's ')

if countStats:
    print()
    print('Write statistics to database')
    print(time.ctime())
    print()
    fx.writeStats(sqlDB)

fx.runLock('off')  # remove run lock

print('Finished run')
print(time.ctime())
print()
