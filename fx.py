# -*- coding: utf-8 -*-

"""

Stephen Po-Chedley 9 May 2019

This is a library of helper functions for xagg.

Functions:
        lookupCMIPMetadata
        produceCMIP5Activity
        ensure_dir
        createGridLabel
        parsePath
        scantree
        toSQLtime
        sqltimeToDatetime
        findDiskPaths
        getDBPaths
        getInvalidDBPaths
        getRetiredDBPaths
        initializeDB
        parallelFindData
        createLookupDictionary
        getCMIPMeta
        createFilename
        xmlWrite
        errorLogic
        getWarnings
        parseWarnings
        sqlUpdate
        sqlInsert
        process_path
        updateDatabaseHoldings
        getScanList
        writeStats
        scanChunk
        writeScanResults
        resetXmlsByQuery
        removeDatabasePathsByQuery
        runLock

@author: pochedls
"""

import scandir
import sqlite3
import numpy as np
import re
import json
import datetime
from joblib import Parallel, delayed
import multiprocessing
import pickle
import re
import os
from subprocess import Popen,PIPE
import datetime
import time
import glob



def lookupCMIPMetadata(mip_era, cmipTable, variable, dictObj={}):
    """
    frequency, realm, dimensions = lookupCMIPMetadata(mip_era, cmipTable, variable, dictObj={})

    This function helps gather CMIP5/6 metadata needed to create the grid label.
    It will use a pickle dictionary if provided, otherwise it gets the information
    from the CMIP tables. Using the pickle file is much faster.

    To create the pickle file, use: createLookupDictionary

    To download the CMIP tables, use: ../tools/updateTables.sh

    Inputs:
            mip_era: 'CMIP5' | 'CMIP6'
            cmipTable: e.g. 'Amon'
            variable: e.g. 'ta'
            dictObj: pickle file (dictionary) that returns appropriate outputs for given inputs.

    Returns (strings):
            frequency, realm, dimensions
    """
    key = mip_era + '.' + cmipTable + '.' + variable
    if key in dictObj:
        frequency, realm, dimensions = dictObj[key]
        return frequency, realm, dimensions
    # https://github.com/PCMDI/cmip6-cmor-tables
    frequency = []
    realm = []
    dimensions = []
    if mip_era == 'CMIP6':
        fn = 'data/cmip6/CMIP6_' + cmipTable + '.json'
        with open(fn) as f:
            data = json.load(f)
        frequency = data['variable_entry'][variable]['frequency']
        realm = data['variable_entry'][variable]['modeling_realm'].split(' ')[0]
        dimensions = data['variable_entry'][variable]['dimensions'].split(' ')
    elif mip_era == 'CMIP5':
        fn = 'data/cmip5/CMIP5_' + cmipTable
        f = open(fn, "r")
        lines = f.readlines()
        f.close()
        inVar = False
        for line in lines:
            if line.find('frequency:') >= 0:
                frequency = line.split(' ')[1].split('\n')[0]
            if line.find('variable_entry:') >= 0:
                if line.find(variable + '\n') >= 0:
                    inVar = True
            if ((line.find('modeling_realm:') >= 0) & (inVar)):
                realm = line.split(' ')[-1].split('\n')[0]
            if ((line.find('dimensions:') >= 0) & (inVar)):
                dimensions = line.split('  ')[-1].split('\n')[0].split(' ')
                break

    return frequency, realm, dimensions

def produceCMIP5Activity(experiment):
    """
    activity = produceCMIP5Activity(experiment)

    This function returns the appropriate activity for a given experiment. This
    is essentially a hardcoded dictionary.

    Inputs:
            experiment (string): e.g. 'historical'

    Returns:
            activity (string)
    """
    activityTable = {'sst2030' : 'CFMIP', 'sstClim' : 'RFMIP', 'sstClim4xCO2' : 'RFMIP',
                    'sstClimAerosol' : 'RFMIP', 'sstClimSulfate' : 'RFMIP',
                    'amip4xCO2' : 'CFMIP', 'amipFuture' : 'CFMIP', 'aquaControl' : 'CFMIP',
                    'aqua4xCO2' : 'CFMIP', 'aqua4K' : 'CFMIP', 'amip4K' : 'CFMIP',
                    'piControl' : 'CMIP', 'historical' : 'CMIP', 'esmControl' : 'CMIP',
                    'esmHistorical' : 'CMIP', '1pctCO2' : 'CMIP', 'abrupt4xCO2' : 'CMIP',
                    'amip' : 'CMIP', 'historicalExt' : 'CMIP', 'esmrcp85' : 'C4MIP',
                    'esmFixClim1' : 'C4MIP', 'esmFixClim2' : 'C4MIP', 'esmFdbk1' : 'C4MIP',
                    'esmFdbk2' : 'C4MIP', 'historicalNat' : 'DAMIP', 'historicalGHG' : 'DAMIP',
                    'historicalMisc' : 'DAMIP', 'midHolocene' : 'PMIP', 'lgm' : 'PMIP',
                    'past1000' : 'PMIP', 'rcp45' : 'ScenarioMIP', 'rcp85' : 'ScenarioMIP',
                    'rcp26' : 'ScenarioMIP', 'rcp60' : 'ScenarioMIP'}

    reDec = re.compile(r'decadal[0-9]{4}')
    if not not re.search(reDec, experiment):
        activity = 'DCPP'
    elif experiment in activityTable.keys():
        activity = activityTable[experiment]
    else:
        activity = 'CMIP5'

    return activity

def ensure_dir(file_path):
    """

    Function ensures there is a directory for a given file.

    """
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError:
            pass

def createGridLabel(mip_era, realm, cmipTable, grid, dimensions):
    """

    gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)

    This function creates a grid label.

    Inputs:
            mip_era: 'CMIP5' | 'CMIP6'
            realm: e.g. 'atmos'
            cmipTable: e.g. 'Amon'
            grid: e.g. 'gm'
            dimensions (list): e.g. ['longitude', 'latitude', 'plevs', 'time']

    Returns:
            gridLabel

    Based on: https://docs.google.com/document/d/1bUwK6G_fVZO53UjLZbQUOuBP47PsT8lqKKhL1pjRnKg/edit

    """
    # get realm id
    realmIdLookup = {'aerosol' : 'ae', 'atmos' : 'ap', 'atmosChem' : 'ac',
                    'land' : 'ld', 'landIce' : 'gi', 'seaIce' : 'si',
                    'ocean' : 'op', 'ocnBgchem' : 'oc', 'river' : 'rr'}
    # realmId = realmIdLookup[realm]

    # vert-id lookup information
    z1List = set(['height2m', 'height10m', 'depth0m', 'depth100m', 'olayer100m', 'sdepth1', 'sdepth10', 'height100m', 'depth300m', 'depth700m', 'depth2000m'])
    lList = set(['olevel', 'olevhalf', 'alevel', 'alevhalf'])
    reP = re.compile(r'p[0-9]')
    pCheck = [not not re.search(reP,i) for i in dimensions]
    rePl = re.compile(r'pl[0-9]')
    plCheck = [not not re.search(rePl,i) for i in dimensions]
    rePlev = re.compile(r'plev[0-9]')
    plevCheck = [not not re.search(rePlev,i) for i in dimensions]

    # get vert id
    if  len(dimensions) == 0:
        vertId = 'x'
    elif len(z1List.intersection(set(dimensions))) > 0:
        vertId = 'z1'
    elif (any(pCheck) | any(plCheck)):
        vertId = 'p1'
    elif len(lList.intersection(set(dimensions))) > 0:
        vertId = 'l'
    elif 'alev1'in dimensions:
        vertId = 'l1'
    elif 'sdepth'in dimensions:
        vertId = 'z'
    elif 'alt16'in dimensions:
        vertId = 'z16'
    elif 'alt40'in dimensions:
        vertId = 'z40'
    elif 'rho'in dimensions:
        vertId = 'd'
    elif 'plevs' in dimensions:
        vertId = 'p17'
    elif any(plevCheck):
        dimensions = np.array(dimensions)
        vertId = 'p' + dimensions[plevCheck][0].split('plev')[1]
    else:
        vertId = '2d'

    # get region id
    if mip_era == 'CMIP6':
        if cmipTable in ['IfxAnt', 'IyrAnt', 'ImonAnt']:
            regionId = 'ant'
        elif cmipTable in ['IfxGre', 'IyrGre', 'ImonGre']:
            regionId = 'gre'
        else:
            regionId = 'glb'
    else:
        regionId = 'glb'

    # get h1 variable
    locList = set(['site', 'oline', 'basin', 'siline', 'location'])
    dimList = set(['latitude', 'yant', 'ygre', 'longitude', 'xant', 'yant'])
    if len(locList.intersection(set(dimensions))) > 0:
        h1 = 's'
    elif cmipTable in ('AERmonZ', 'E6hrZ', 'EdayZ', 'EmonZ'):
        h1 = 'z'
    elif len(dimList.intersection(set(dimensions))) == 0:
        h1 = 'm'
    else:
        h1 = 'g'

    gridLabel = regionId + '-' + vertId + '-' + h1

    if grid == 'gm':
        gridStrip = 'n'
    else:
        gridStrip = grid.replace('g','').replace('a','').replace('z','')

    gridLabel  = gridLabel + gridStrip

    return gridLabel


def parsePath(path, dictObj={}):
    """

    validPath, keyId, mip_era, activity, institute, \
    model, experiment, member, cmipTable, realm, \
    frequency, variable, grid, gridLabel, version = parsePath(path, dictObj={})

    This function parses a directory path for CMIP metadata. A pickle file
    can optionally be provided to lookup missing CMIP metadata. If this is not
    provided it will search the CMIP table information for the necesary information,
    which is much slower (see lookupCMIPMetadata).

    Inputs:
            path (string)
            dictObj (string of filename)

    Returns:
            validPath, keyId, mip_era, activity, institute, model, experiment, ...
            member, cmipTable, realm, frequency, variable, grid, gridLabel, version

    Note that validPath is a boolean denoting whether a path contains valid data (True)
    or not (False) based on logic from the path only (it doesn't check for netCDF files).

    """
    meta = path.split('/')[1:-1]

    validPath = True
    # remove double versions
    if meta[-2] == meta[-3]:
        meta.pop(-2)
    # check for 'bad' directories
    e = path.split('/')[-2]
    bad = re.compile('bad[0-9]{1}')
    check = re.match(bad, e)
    checkBad = True
    if check != None:
        checkBad = False
    if ((len(meta) > 10) & (checkBad)):
        if meta[-10].upper() == 'CMIP6':
            version = meta[-1]
            grid = meta[-2]
            variable = meta[-3]
            cmipTable = meta[-4]
            member = meta[-5]
            experiment = meta[-6]
            model = meta[-7]
            institute = meta[-8]
            activity = meta[-9]
            mip_era = meta[-10].upper()
            try:
                frequency, realm, dimensions = lookupCMIPMetadata(mip_era, cmipTable, variable, dictObj)
                gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)
            except:
                realm = 'unk'
                frequency = 'unk'
                gridLabel = 'unk'
        elif ((meta[-1] != '1') & (meta[-1] != '2')):
            variable = meta[-1]
            version = meta[-2]
            member = meta[-3]
            cmipTable = meta[-4]
            realm = meta[-5]
            frequency = meta[-6]
            experiment = meta[-7]
            model = meta[-8]
            institute = meta[-9]
            activity = produceCMIP5Activity(experiment)
            mip_era = 'CMIP5'
            grid = 'gu'
            frequencyx, realmx, dimensions = lookupCMIPMetadata(mip_era, cmipTable, variable, dictObj)
            if frequency == 'monClim':
                frequency = 'monC'
            if 'time1' in dimensions:
                frequency = frequency + 'Pt'
            gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)
        else:
            variable = meta[-2]
            version = meta[-1]
            member = meta[-3]
            cmipTable = meta[-4]
            realm = meta[-5]
            frequency = meta[-6]
            experiment = meta[-7]
            model = meta[-8]
            institute = meta[-9]
            activity = produceCMIP5Activity(experiment)
            mip_era = 'CMIP5'
            grid = 'gu'
            frequencyx, realmx, dimensions = lookupCMIPMetadata(mip_era, cmipTable, variable, dictObj)
            if frequency == 'monClim':
                frequency = 'monC'
            if 'time1' in dimensions:
                frequency = frequency + 'Pt'
            gridLabel = createGridLabel(mip_era, realm, cmipTable, grid, dimensions)
        keyId = [mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version]
        keyId = '.'.join(keyId)
    else:
        validPath = False
        version = []
        grid = []
        variable = []
        cmipTable = []
        realm = []
        frequency = []
        gridLabel = []
        member = []
        experiment = []
        model = []
        institute = []
        activity = []
        mip_era = []
        keyId = []

    return validPath, keyId, mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version


def scantree(path):
    """
    scantree(path)

    This is an iterator method that recursively scans for directories that meet the
    following criteria:
        1. The directory has no sub-directories
        2. The directory contains files

    It is based on:
        https://stackoverflow.com/questions/33135038/how-do-i-use-os-scandir-to-return-direntry-objects-recursively-on-a-directory

    """

    for entry in scandir.walk(path):
        if ((entry[1] == []) & (entry[2] != [])):
            yield entry[0] + '/'

def toSQLtime(time):
    '''
    sqlTime = toSQLtime(time)

    Function takes a datetime object and returns a SQL-like datestring
    '''
    time = "{:02d}".format(time.year) + '-' + "{:02d}".format(time.month) + '-' + "{:02d}".format(time.day) + ' ' + "{:02d}".format(time.hour) + ':' + "{:02d}".format(time.minute) + ':' + "{:02d}".format(time.second)

    return time

def sqltimeToDatetime(sqltime):
    '''
    time = sqltimeToDatetime(sqltime)

    Function takes a SQL-like datestring and returns a datetime object
    '''
    d = sqltime.split(' ')[0]
    t = sqltime.split(' ')[1]
    y = int(d.split('-')[0])
    mth = int(d.split('-')[1])
    d = int(d.split('-')[2])
    h = int(t.split(':')[0])
    m = int(t.split(':')[1])
    s = int(t.split(':')[2])
    return datetime.datetime(y,mth,d,h,m,s)

def findDiskPaths(path):
    '''
    dpaths = findDiskPaths(path)

    Function uses the scantree iterator to check all eligible paths
    that fall under a parent path (input: path) for their created (ctime),
    modified (mtime), and accessed (atime) times. It returns this in a dictionary
    object:

    Returns:
        dbPaths[childPath] = {'ctime' : ctime, 'mtime' : mtime, 'atime' : atime}

    '''
    x = scantree(path)
    dpaths = {}
    s = time.time()
    for file_path in x:
        ts = scandir.stat(file_path)
        ctime = toSQLtime(datetime.datetime.fromtimestamp(ts.st_ctime))
        mtime = toSQLtime(datetime.datetime.fromtimestamp(ts.st_mtime))
        atime = toSQLtime(datetime.datetime.fromtimestamp(ts.st_atime))
        dpaths[file_path] = {'ctime' : ctime, 'mtime' : mtime, 'atime' : atime}
    e = time.time()
    print(path, e-s)
    return dpaths

def getDBPaths(sqlDB):
    '''
    db = getDBPaths(sqlDB)

    Function uses the specified sql database and returns all information
    about each path in the database in a dictionary. The dictionary key
    is the path.

    Input:
        sqlDb (string): filename of sqlite file

    Returns:
        db[path]

    db[path] is then a sub-dictionary that contains all columns in the database:

        keyid, mip_era, activity, institute, model, experiment, member, cmipTable, realm, ...
        frequency, variable, grid, gridLabel, version, created, modified, accessed, xmlFile, ...
        xmlwritedatetime, error, retired, retire_datetime, ignored, ignored_datetime

    '''
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # Get all paths
    c.execute('select * from paths;')
    a = c.fetchall()
    conn.close()
    db = {}
    for row in a:
        db[row[0]] = {'keyid' : row[1], 'mip_era' : row[2], 'activity' : row[3], 'institute' : row[4], 'model' : row[5], 'experiment' : row[6], 'member' : row[7], 'cmipTable' : row[8], 'realm' : row[9], 'frequency' : row[10], 'variable' : row[11], 'grid' : row[12], 'gridLabel' : row[13], 'version' : row[14], 'created' : row[15], 'modified' : row[16], 'accessed' : row[17], 'xmlFile' : row[18], 'xmlwritedatetime' : row[19], 'error' : row[20], 'retired' : row[21], 'retire_datetime' : row[22], 'ignored' : row[23], 'ignored_datetime' : row[23]}
    return db

def getInvalidDBPaths(sqlDB):
    '''
    db = getInvalidDBPaths(sqlDB)

    Function uses the specified sql database and returns a list
    of paths that are known to be invalid.

    Input:
        sqlDb (string): filename of sqlite file

    Returns:
        invalidPaths (list)

    '''
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # Create table
    c.execute('select path from invalid_paths;')
    a = c.fetchall()
    conn.close()
    db = []
    for row in a:
        db.append(row[0])
    return db

def getRetiredDBPaths(sqlDB):
    '''
    db = getRetiredDBPaths(sqlDB)

    Function uses the specified sql database and returns a list
    of paths that are known to be retired.

    Input:
        sqlDb (string): filename of sqlite file

    Returns:
        retiredPaths (list)

    '''
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # Create table
    c.execute('select path from paths where retired = 1;')
    a = c.fetchall()
    conn.close()
    db = []
    for row in a:
        db.append(row[0])
    return db

def initializeDB(sqlDB):
    '''
    initializeDB(sqlDB)

    Function initialized a sqlite database with the correct
    tables to be used with xagg software.

    Input:
        sqlDb (string): filename of sqlite file

    '''
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # Create table
    c.execute('''drop table if exists paths;''')
    c.execute('''CREATE TABLE paths
                 (path varchar(255), keyid varchar(255), mip_era varchar(255), activity varchar(255), institute varchar(255), model varchar(255), experiment varchar(255), member varchar(255), cmipTable varchar(255), realm varchar(255), frequency varchar(255), variable varchar(255), grid varchar(255), gridLabel varchar(255), version varchar(255), created datetime, modified datetime, accessed datetime, xmlFile varchar(255), xmlwritedatetime datetime, error varchar(255), retired BOOLEAN, retire_datetime datetime, ignored BOOLEAN, ignored_datetime DATETIME)''')
    c.execute('''CREATE INDEX pathIndex ON paths (path);''')
    c.execute('''drop table if exists invalid_paths;''')
    c.execute('''CREATE TABLE invalid_paths (path varchar(255), datetime DATETIME)''')
    c.execute('''drop table if exists stats;''')
    c.execute('''CREATE TABLE stats (indicator varchar(255), value int, datetime DATETIME)''')
    c.execute('''drop table if exists runs;''')
    c.execute('''CREATE TABLE runs (datetime DATETIME, total INT, new INT, invalid INT, modified INT, missing INT, returned INT, deleted INT)''')
    # Save (commit) the changes
    conn.commit()
    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()

def parallelFindData(data_directories, numProcessors=20, split=[], rmDir=[]):
    '''
    diskStat = parallelFindData(data_directories, numProcessors=20, split=[], rmDir=[])

    Function simply parallelizes findDiskPaths to speed up the search for eligible paths.

    Input:
        data_directories (list): list of parent directories to search
        num_processors (int): number of processors to use (default 20)
        split (list): list of parent directories that should be split apart into separate threads
                      e.g., '/parent/' -> ['parent/child1', 'parent/child2']
        rmDir (list): list of directories to ignore while scanning

    Returns:
        dbPaths[childPath] = {'ctime' : ctime, 'mtime' : mtime, 'atime' : atime}

    '''
    # grab the right number of processors
    for d in split:
        dlist = glob.glob(d + '/*/')
        data_directories = data_directories + dlist
    for d in rmDir:
        data_directories.remove(d)
    if len(data_directories) > numProcessors:
        nfscan = numProcessors
    else:
        nfscan = len(data_directories)

    print('Using ' + str(nfscan) + ' processors to check ' + str(len(data_directories)) + ' directories...', end='\n \n')

    results = Parallel(n_jobs=nfscan)(delayed(findDiskPaths)(parent)\
           for (parent) in data_directories)
    diskStat = {}
    for d in results:
        keys = d.keys()
        for key in keys:
            diskStat[key] = d[key]
    return diskStat

def createLookupDictionary(paths, outfile='data/cmipMeta.pkl'):
    """
    createLookupDictionary(paths, outfile='data/cmipMeta.pkl')

    Function processes a bunch of paths and stores a dictionary file with
    metadata lookup information. This allows you to quickly determine a path's
    frequency, realm, and dimensions (which are not contained in the directory
    structure).

    Inputs:
            paths (list)
            outfile (string filename), default 'data/cmipMeta.pkl'
            variable: e.g. 'ta'
            dictObj: pickle file (dictionary) that returns appropriate outputs for given inputs.
    """
    dictObj = {}
    for i, p in enumerate(paths):
        if np.mod(i,10000) == 0:
            print(str(i) + '/' + str(len(paths)))
        validPath, keyId, mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version = parsePath(p)
        if validPath:
            key = '.'.join([mip_era, cmipTable, variable])
        else:
            continue
        if key in dictObj:
            continue
        else:
            try:
                frequency, realm, dimensions = lookupCMIPMetadata(mip_era, cmipTable, variable)
                dictObj[key] = [frequency, realm, dimensions]
            except:
                continue
    with open(outfile, 'wb') as f:
        pickle.dump(dictObj, f, pickle.HIGHEST_PROTOCOL)

def getCMIPMeta(outfile='data/cmipMeta.pkl'):
    """
    dictObj = getCMIPMeta(outfile='data/cmipMeta.pkl')

    Function reads the CMIP Metadata pickle file and returns the information
    as an in-memory dictionary.

    """
    with open(outfile, 'rb') as f:
        dictObj = pickle.load(f)
    return dictObj

def createFilename(xmlOutputDir, pathMeta):
    """
    fn = createFilename(xmlOutputDir, pathMeta)

    Function contains the logic to create output filenames for a given output
    directory and path metadata.

    Inputs:
        xmlOutputDir (string): directory which contains the xml file tree (e.g., '/data/goes/here/')
        pathMeta (dictionary): dictionary object containing necesary metadata to create filename (specified below)

    Returns:
        fn (string)

    pathMeta contains the following keys:
        mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version

    """
    reDec = re.compile(r'decadal[0-9]{4}')
    if not not re.search(reDec, pathMeta['experiment']):
        experimentPath = 'decadal'
    else:
        experimentPath = pathMeta['experiment']
    # output filename
    if pathMeta['frequency'] == 'fx':
        fn = xmlOutputDir + '/' + pathMeta['mip_era'] + '/' + pathMeta['frequency'] + '/' + pathMeta['variable'] + '/' + pathMeta['mip_era'] + '.' + pathMeta['activity'] + '.' + pathMeta['experiment'] + '.' + pathMeta['institute'] + '.' + pathMeta['model'] + '.' + pathMeta['member'] + '.' + pathMeta['frequency'] + '.' + pathMeta['variable'] + '.' + pathMeta['realm'] + '.' + pathMeta['gridLabel'] + '.' + pathMeta['version'] + '.0000000.0.xml'
    else:
        fn = xmlOutputDir + '/' + pathMeta['mip_era'] + '/' + pathMeta['activity'] + '/' + experimentPath + '/' + pathMeta['realm'] + '/' + pathMeta['frequency'] + '/' + pathMeta['variable'] + '/' + pathMeta['mip_era'] + '.' + pathMeta['activity'] + '.' + pathMeta['experiment'] + '.' + pathMeta['institute'] + '.' + pathMeta['model'] + '.' + pathMeta['member'] + '.' + pathMeta['frequency'] + '.' + pathMeta['variable'] + '.' + pathMeta['realm'] + '.' + pathMeta['gridLabel'] + '.' + pathMeta['version'] + '.0000000.0.xml'
    fn = fn.replace('//','/')

    return fn

def xmlWrite(inpath, outfile):
    """
    xmlWrite(inpath, outfile)

    Function calls cdscan to create an xml file (outfile) for a given
    directory of CMIP data (inpath).

    Inputs:
        inpath (string): directory containing input files
        outfile (string): xml file to write

    Returns:
        out, err: command output and error message strings

    """
    cmd = 'cdscan -x ' + outfile + ' ' + inpath + '/*.nc'
    cmd = cmd.replace('//', '/')
    p = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE)
    out,err = p.communicate()
    return out, err

def errorLogic(fn, inpath, err):
    """
    fn, err = errorLogic(fn, inpath, err)

    Function processes error messages and groups the errors into a subset of categories. It then
    renames the xmlFile (if present) to reflect these errors.

    Inputs:
        fn (string): xml filename
        inpath: path of input netCDF data
        err: error message returned during xmlWrite call

    Returns:
        fn: final filename
        error: processed error message

    """
    # check for zero sized files and other no write errors (if there xml did not write)
    zeroSize = False
    error = None
    if not os.path.exists(fn):
        fiter = glob.glob(inpath + '/*.nc')
        fn = None
        for fnc in fiter:
            fsize = os.path.getsize(fnc)
            if fsize == 0:
                zeroSize = True
                break
        if zeroSize:
            error = 'No write: filesize of zero'
        elif str(err).find('CDMS I/O error: End of file') >= 0:
            error = 'No write: CDMS I/O Error'
        elif str(err).find('RuntimeError: Dimension time in files') >= 0:
            error = 'RuntimeError: Dimension time in files'
        elif str(err).find('CDMS I/O error: Determining type of file') >= 0:
            error = 'CDMS I/O error: Determining type of file'
        elif str(err).find('Cannot allocate memory') >= 0:
            error = 'Cannot allocate memory'
        elif str(err).find('Invalid relative time units') >= 0:
            error = 'Invalid relative time units'
        else:
            error = 'No write'
    else:
        # if xml wrote to disk, update error codes in filename
        errors = getWarnings(str(err))
        if len(errors) > 0:
            errorCode = parseWarnings(errors)
            fnNew = fn.replace('0000000',errorCode)
            os.rename(fn, fnNew)
            fn = fnNew
            if len(errors) > 255:
                errors = errors[0:255]
                error = errors
    return fn, error

def getWarnings(err):
    """
    errorCode = getWarnings(err)

    Function parses warning messages for more specific error.

    Inputs:
        err: error message returned during xmlWrite call

    Returns:
        errorCode: subsetted warning message

    """
    errstart = err.find('Warning') ; # Indexing returns value for "W" of warning
    err1 = err.find(' to: [')
    if err1 == -1: err1 = len(err)-1 ; # Warning resetting axis time values
    err2 = err.find(': [')
    if err2 == -1: err2 = len(err)-1 ; # Problem 2 - cdscan error - Warning: axis values for axis time are not monotonic: [
    err3 = err.find(':  [')
    if err3 == -1: err3 = len(err)-1 ; # Problem 2 - cdscan error - Warning: resetting latitude values:  [
    err4 = err.find(') ')
    if err4 == -1: err4 = len(err)-1 ; # Problem 1 - zero infile size ; Problem 4 - no outfile
    err5 = err.find(', value=')
    if err5 == -1: err5 = len(err)-1 ; # Problem 2 - cdscan error - 'Warning, file XXXX, dimension time overlaps file
    errorCode = err[errstart:min(err1,err2,err3,err4,err5)]

    errPython = err.find('Traceback (most recent call last)')
    if errPython > 0:
        errorCode = errorCode + 'Python Error'

    return errorCode

def parseWarnings(err):
    """
    errorCode = parseWarnings(err)

    Function parses subsetted warning messages for exact error code [0-6].

    Inputs:
        err: Subsetted warning from getWarning.

    Returns:
        errorCode: string specifying error category (details below)

    '0000000'   - no warnings
     1          - dimension time contains values...
      1         - Warning, Axis values for axis are not monotonic...
       1        - Warning: resetting latitude values...
        1       - Zero infile size
         1      - Dimension time overlaps...
          1     - Your first bounds...
           1    - Python error ...

    """
    errorCode = list('0000000')
    if err.find('dimension time contains values in file') >= 0:
        errorCode[0] = '1'
    if err.find('Warning: Axis values for axis time are not monotonic') >= 0:
        errorCode[1] = '1'
    if err.find('Warning: resetting latitude values') >= 0:
        errorCode[2] = '1'
    if err.find('zero infile size') >= 0:
        errorCode[3] = '1'
    if err.find('dimension time overlaps file') + err.find('dimension time contains values in file') >= 0:
        errorCode[4] = '1'
    if err.find('Your first bounds') >= 0:
        errorCode[5] = '1'
    if err.find('Python Error') >= 0:
        errorCode[6] = '1'
    errorCode = "".join(errorCode)
    return errorCode

def sqlUpdate(sqlDB, table, columns, constraint, datalist):
    """
    sqlUpdate(sqlDB, table, columns, constraint, datalist)

    Function will update rows in sqlite database with new information.

    Inputs:
        sqlDB: string filename
        table (string): table to update
        columns (list): columns to update
        constraint (string): column which is used as a constraint
        dataList: list containing new data

    dataList is a list such that each row contains columns to update
    followed by the constraint
        dataList: ['blue', None, 'audi', 5]
    where 'newData', NoneN, and 'otherNewData' are the updated information and
    5 is the constraint. This could correspond to a constraint like 'number'
    and columns like ['color', 'animal', 'model_car'].

    """
    if type(columns) == str:
        columns = [columns]
    q = 'UPDATE ' + table + ' SET ' + '=?, '.join(columns) + '=? WHERE ' + constraint + '=?;'
    # connect to db
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # update data
    if len(datalist) > 1000:
        for i in range(int(np.ceil(len(datalist)/1000))):
            ro = datalist[i*1000:i*1000+1000]
            c.executemany(q, ro)
            conn.commit()
    else:
        c.executemany(q, datalist)
        conn.commit()
    conn.close() # close connection

def sqlInsert(sqlDB, table, columns, datalist):
    """
    sqlInsert(sqlDB, table, columns, datalist)

    Function will update rows in sqlite database with new information.

    Inputs:
        sqlDB: string filename
        table (string): table to update
        columns (list): columns to update
        dataList: list containing new data

    dataList is a list such that each row contains columns to insert
        dataList: ['blue', None, 'audi', 5]
    where 'newData', None, and 'otherNewData' are the new information.
    This could correspond to columns like ['color', 'animal', 'model_car'].

    """
    q = 'INSERT INTO ' + table + '(' + ', '.join(columns) + ') VALUES(' + '?,' * (len(columns) - 1) + '?);'
    # connect to db
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # update data
    c.executemany(q, datalist)
    conn.commit() # commit changes
    conn.close() # close connection

def process_path(xmlOutputDir, pathMeta, inpath):
    """
    inpath, fn, xmlwritetime, error = process_path(xmlOutputDir, pathMeta, inpath)

    Function processes a path by creating an output filename for the xml file,
    creates an xml file for a given directory, and processes any error messages.

    Inputs:
        xmlOutputDir (string): base directory for xml tree
        pathMeta (dictionary): dictionary object containing necesary metadata to create filename (specified below)
        inpath (string): directory which contains the netCDF data to process

    Returns:
        inpath (string): directory which contains the netCDF data to process
        fn (string): filename of xmlfile written out (or None if applicable)
        xmlwritetime: sql datetime string of xml write time
        error: parsed error message

    pathMeta contains the following keys:
        mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version

    """
    fn = createFilename(xmlOutputDir, pathMeta)
    ensure_dir(fn)
    out,err = xmlWrite(inpath, fn)
    # get write time
    xmlwritetime = toSQLtime(datetime.datetime.now())
    # get warnings
    fn, error = errorLogic(fn, inpath, err)

    # update database with xml write
    return inpath, fn, xmlwritetime, error

def updateDatabaseHoldings(sqlDB, diskPaths, diskStat, dbPaths, db, invalidPaths, retiredPaths, quiet=False):
    """
    updateDatabaseHoldings(sqlDB, diskPaths, diskStat, dbPaths, db, invalidPaths, retiredPaths, quiet=False)

    Function updates all database information based on scan of all disk paths. Functionality includes:
        * Will kill job if more than 10% of data isn't in diskPaths
        * Inserts new paths into database (with appropriate metadata)
        * Updates modified times on modified directories
        * Updates invalidPaths with new invalid paths
        * Will retire paths that are removed and delete underlying xmls
        * Will "un-retire" a path that re-appears on the disk
        * Will delete xmls where path is modified (for later re-creation)
        * Prints out relevant information and records this information in the database (runs table)
            * Total Scanned, New, Invalid, Modified, Missing, Returned, Deleted

    Inputs:
        sqlDB: string filename
        diskPaths: keys of all disk paths
        diskStat: Dictionary of stat information for each path (see findDiskPaths)
        dbPaths: keys of all paths in the database
        db: database dictionary object (see getDBPaths)
        invalidPaths: list of invalid paths (see getInvalidDBPaths)
        retiredPaths: list of retired paths (see getRetiredDBPaths)
        quiet (optional, boolean): suppress display information if True (default False)

    """
    # create sets for lists to speed up logic
    retiredPaths = set(retiredPaths)
    invalidPaths = set(invalidPaths)

    # Sanity check to make sure disks aren't unmounted
    if len(diskPaths) < (len(dbPaths) - len(retiredPaths)) * 0.9:
        raise ValueError('A large number of paths are missing - check disks')

    # Bin paths into appropriate categories
    newPaths = []
    modifiedPaths = []
    unretirePaths = []
    for p in diskPaths:
        if p in retiredPaths:
            unretirePaths.append(p)
        if p in invalidPaths:
            continue
        if p in dbPaths:
            if sqltimeToDatetime(diskStat[p]['mtime']) > sqltimeToDatetime(db[p]['modified']):
                modifiedPaths.append(p)
        else:
            newPaths.append(p)

    missingPaths = []
    for p in dbPaths:
        if p in retiredPaths:
            continue
        if p not in diskPaths:
            missingPaths.append(p)

    newCount = 0
    invalidCount = 0
    modifiedCount = len(modifiedPaths)
    missingCount = len(missingPaths)
    unretiredCount = len(unretirePaths)
    deleteCount = 0

    ## process new records (not in database)
    if len(newPaths) > 0:
        if not quiet:
            print('Parsing data paths not in database')
            print(time.ctime())
            print()
        # get metadata object
        dictObj = getCMIPMeta()
        newList = []
        invalidList = []
        for p in newPaths:
            try:
                validPath, keyId, mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version = parsePath(p, dictObj)
            except:
                print(p)
                stop
            if validPath:
                litem = [p, keyId, mip_era, activity, institute, model, experiment, member, cmipTable, realm, frequency, variable, grid, gridLabel, version, diskStat[p]['ctime'], diskStat[p]['mtime'], diskStat[p]['atime'], 0, 0]
                newList.append(litem)
            else:
                invalidList.append([p, toSQLtime(datetime.datetime.now())])

        newCount = len(newList)
        invalidCount = len(invalidList)

        # write new paths to database
        if not quiet:
            print('Writing new paths')
            print(time.ctime())
            print()
        columns = ['path', 'keyId', 'mip_era', 'activity', 'institute', 'model', 'experiment', 'member', 'cmipTable', 'realm', 'frequency', 'variable', 'grid', 'gridLabel', 'version', 'created', 'modified', 'accessed', 'retired', 'ignored']
        sqlInsert(sqlDB, 'paths', columns, newList)
        # write invalid paths to database
        sqlInsert(sqlDB, 'invalid_paths', ['path', 'datetime'], invalidList)

    ## process paths with modified timestamp
    if modifiedCount > 0:
        if not quiet:
            print('Updating modified paths')
            print(time.ctime())
            print()

        # update modified paths in database
        modifiedList = []
        for p in modifiedPaths:
            litem = [diskStat[p]['ctime'], diskStat[p]['mtime'], diskStat[p]['atime'], None, None, None, 0, None, 0, None, p]
            modifiedList.append(litem)

        columns = ['created', 'modified', 'accessed', 'xmlFile', 'xmlwritedatetime', 'error', 'retired', 'retire_datetime', 'ignored', 'ignored_datetime']
        sqlUpdate(sqlDB, 'paths', columns, 'path', modifiedList)

    ## process retired paths
    if unretiredCount > 0:
        if not quiet:
            print('Unretiring relevant paths')
            print(time.ctime())
            print()

        ## update retired paths in database
        unretiredList = []
        for p in unretirePaths:
            litem = [diskStat[p]['ctime'], diskStat[p]['mtime'], diskStat[p]['atime'], None, None, None, 0, None, 0, None, p]
            unretiredList.append(litem)

        columns = ['created', 'modified', 'accessed', 'xmlFile', 'xmlwritedatetime', 'error', 'retired', 'retire_datetime', 'ignored', 'ignored_datetime']
        sqlUpdate(sqlDB, 'paths', columns, 'path', unretiredList)

    ## process paths where directory is missing
    if missingCount > 0:
        if not quiet:
            print('Retiring missing data')
            print(time.ctime())
            print()

        # update missing paths in database

        columns = ['xmlFile', 'xmlwritedatetime', 'error', 'retired', 'retire_datetime', 'ignored', 'ignored_datetime']
        missingList = []
        for p in missingPaths:
            litem = [None, None, None, 1, toSQLtime(datetime.datetime.now()), 0, None, p]
            missingList.append(litem)

        sqlUpdate(sqlDB, 'paths', columns, 'path', missingList)

    ## Delete retired / modified xml files
    if (missingCount + modifiedCount) > 0:
        if not quiet:
            print('Removing xml files for modified / missing data')
            print(time.ctime())
            print()

        for p in missingPaths:
            xfn = db[p]['xmlFile']
            if xfn is not None:
                if os.path.exists(xfn):
                    os.remove(xfn)
                    deleteCount += 1

        for p in modifiedPaths:
            xfn = db[p]['xmlFile']
            if xfn is not None:
                if os.path.exists(xfn):
                    os.remove(xfn)
                    deleteCount += 1

    ## Write out
    columns = ['datetime', 'total', 'new', 'invalid', 'modified', 'missing', 'returned', 'deleted']
    row = [[toSQLtime(datetime.datetime.now()), len(diskPaths), newCount, invalidCount, modifiedCount, missingCount, unretiredCount, deleteCount]]
    sqlInsert(sqlDB, 'runs', columns, row)

    if not quiet:
        print('Total Scanned   : ' + str(len(diskPaths)))
        print('   New          : ' + str(newCount))
        print('   Invalid      : ' + str(invalidCount))
        print('   Modified     : ' + str(modifiedCount))
        print('   Missing      : ' + str(missingCount))
        print('   Returned     : ' + str(unretiredCount))
        print('   Deleted      : ' + str(deleteCount))
        print()

def getScanList(sqlDB, db, variables, experiments, frequencies, quiet=False):
    """
    getScanList(sqlDB, db, variables, experiments, frequencies, quiet=False)

    Function will produce a dictionary of paths to scan that includes all of the metadata
    about each path. The directories included meet this criteria:
        * Include the variables, experiments, and frequencies supplied
        * The path was last modified more than 24 hours ago
        * Path is not ignored or retired
        * If multiple paths have the same metadata, only one is listed for scanning

    Inputs:
        sqlDB: string filename
        db: database dictionary object (see getDBPaths)
        variables (list): list of variables to scan (e.g., ['tas', 'ta', 'ts'])
        experiments (list): list of experiments to scan (e.g., ['amip', 'historical'])
        frequencies (list): list of frequencies to scan (e.g., ['mon', 'fx'])
        quiet (optional, boolean): suppress display information if True (default False)

    Returns:
        scanList[path] = db[path]

    """

    ## get keys with that have an associated xml file
    conn = sqlite3.connect(sqlDB) # connect to db
    c = conn.cursor()
    # get keys for which we have an xml file
    c.execute('select keyid from paths where xmlFile is not NULL;')
    result = c.fetchall()
    conn.close() # close connection
    # create list
    keyExistList = []
    for row in result:
        keyExistList.append(row[0])

    # paths to scan
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # get list to scan
    vs = '\'' + '\', \''.join(variables) + '\''
    es = '\'' + '\', \''.join(experiments) + '\''
    fs = '\'' + '\', \''.join(frequencies) + '\''
    q = """SELECT keyid, path
           FROM paths
           WHERE xmlfile IS NULL
           AND error is NULL
           AND variable IN (""" + vs + """)
           AND experiment IN (""" + es + """)
           AND frequency IN (""" + fs + """)
           AND retired = 0
           AND ignored = 0
           AND (strftime(\'%s\', \'now\') - strftime(\'%s\', modified))/3600 > 24
           GROUP BY keyid;"""
    c.execute(q)
    result = c.fetchall()
    conn.close() # close connection
    # create scan list
    scanEligible = []
    for row in result:
        scanEligible.append(list(row))

    # get non-duplicate paths to scan
    scanList = []
    for i in range(len(scanEligible)):
        key = scanEligible[i][0]
        p = scanEligible[i][1]
        if key not in keyExistList:
            scanList.append([p, db[p]])
    if not quiet:
        print('Found ' + str(len(scanList)) + ' paths to scan')
        print(time.ctime())
        print()

    return scanList

def writeStats(sqlDB):
    """
    writeStats(sqlDB)

    Function will determines and writes out statistics to the stats table. Statistics currently include:
        * cmip5 directories
        * cmip6 directories
        * cmip5 xml files
        * cmip6 xml files
        * undefined vertical grid (cmip5)
        * undefined vertical grid (cmip6)
    """
    # define queries
    queries = []
    queries.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip5 directories\', (select count(*) as n from paths where mip_era = \'CMIP5\' and retired=0), datetime(\'now\'));")
    queries.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip6 directories\', (select count(*) as n from paths where mip_era = \'CMIP6\' and retired=0), datetime(\'now\'));")
    queries.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip5 xml files\', (select count(*) as n from paths where mip_era = \'CMIP5\' and xmlFile is NOT NULL and xmlFile != 'None' and retired=0), datetime(\'now\'));")
    queries.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'cmip6 xml files\', (select count(*) as n from paths where mip_era = \'CMIP6\' and xmlFile is NOT NULL and xmlFile != 'None' and retired=0), datetime(\'now\'));")
    queries.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'undefined vertical grid (cmip5)\', (select count(*) as n from paths where mip_era = \'CMIP5\' and gridLabel like \'%-%-x-%\' and retired=0), datetime(\'now\'));")
    queries.append("INSERT INTO stats (indicator, value, datetime) VALUES (\'undefined vertical grid (cmip6)\', (select count(*) as n from paths where mip_era = \'CMIP6\' and gridLabel like \'%-%-x-%\' and retired=0), datetime(\'now\'));")
    # write to database
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    # Create table
    for q in queries:
        c.execute(q)
    # Save (commit) the changes
    conn.commit()
    conn.close()

def scanChunk(scanList, numProcessors, outputDirectory):
    """
    results = scanChunk(scanList, numProcessors, outputDirectory)

    Function takes in a scanList (see getScanList) and will pass these directories
    to scan to process path in parallel.

    Inputs:
        scanList[path] = db[path] [dictionary of directories containing all directory metadata in database]
        numProcessors (int): number of processors to use
        outputDirectory: base directory of output xml tree

    Returns:
        list of results from process_path in the form: results[:][inpath, fn, xmlwritetime, error]

    """
    # Determine number of processors
    if len(scanList) > numProcessors:
        nfscan = numProcessors
    else:
        nfscan = len(scanList)

    # parallelize scans
    results = Parallel(n_jobs=nfscan)(delayed(process_path)(outputDirectory, pathMeta, p)\
           for (p, pathMeta) in scanList)

    return results

def writeScanResults(sqlDB, results):
    """
    writeScanResults(sqlDB, results)

    Function takes results from scanChunk and uses them to update the database.

    Inputs:
        sqlDB: string filename
        results: results list from scanChunk

    """
    outputList = []
    for row in results:
        outputList.append(list([row[1], row[2], row[3], row[0]]))

    sqlUpdate(sqlDB, 'paths', ['xmlFile', 'xmlwritedatetime', 'error'], 'path', outputList)

def resetXmlsByQuery(sqlDB, q):
    """
    resetXmlsByQuery(sqlDb, q)

    Takes a sqlDB filename and a query, q, where the query returns the
    path and xmlFile.

    The function then deletes the xmlfile, xmlfilewritedatetime, and errors
    associated with the paths in this query. It then deletes any xmls referenced
    in the query results.  This is used if files need to be rescanned for some
    reason.

    Inputs:
        sqlDB: string filename
        q: string query, e.g., "select path, xmlFile from paths where error is not null and xmlFile is not null;"

    """
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    c.execute(q)
    a = c.fetchall()
    dfiles = []
    plist = []
    for row in a:
        plist.append([None, None, None, row[0]])
        dfiles.append(row[1])
    conn.close()
    n = len(dfiles)
    sqlUpdate(sqlDB, 'paths', ['xmlFile', 'xmlwritedatetime', 'error'], 'path', plist)
    print('Reset ' + str(len(plist)) + ' records in database')
    deleteCount = 0
    for xfn in dfiles:
        if os.path.exists(xfn):
            os.remove(xfn)
            deleteCount += 1
    print('Removed ' + str(deleteCount) + ' xml files')

def ignoreXmlsByQuery(sqlDB, q):
    """
    resetXmlsByQuery(sqlDb, q)

    Takes a sqlDB filename and a query, q, where the query returns the
    path and xmlFile.

    The function then deletes the xmlfile, xmlfilewritedatetime, and errors
    associated with the paths in this query. It sets ignored = 1. It then 
    deletes any xmls referenced in the query results so they will not be 
    scanned in the future. 

    Inputs:
        sqlDB: string filename
        q: string query, e.g., "select path, xmlFile from paths where error is not null and xmlFile is not null;"

    """
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    c.execute(q)
    a = c.fetchall()
    dfiles = []
    plist = []
    for row in a:
        ignoretime = toSQLtime(datetime.datetime.now())
        plist.append([None, None, None,1,ignoretime,row[0]])
        fn = row[1]
        if fn:
            dfiles.append(fn)
    conn.close()
    n = len(dfiles)
    sqlUpdate(sqlDB, 'paths', ['xmlFile', 'xmlwritedatetime', 'error', 'ignored','ignored_datetime'], 'path', plist)
    print('Reset ' + str(len(plist)) + ' records in database')
    deleteCount = 0
    for xfn in dfiles:
        print(xfn)
        if os.path.exists(xfn):
            os.remove(xfn)
            deleteCount += 1
    print('Removed ' + str(deleteCount) + ' xml files')

def removeDatabasePathsByQuery(sqlDB, q):
    """
    removeDatabasePathsByQuery(sqlDb, q)

    Takes a sqlDB filename and a query, q, where the query returns a
    path and xmlFile.

    The function deletes the rows in the database associated with the
    paths in this query. It then deletes any xmls referenced in the query results.
    This is used when you want to completely regenerate this part of the database
    and associated scans.

    Inputs:
        sqlDB: string filename
        q: string query, e.g., "select path, xmlFile from paths where error is not null and xmlFile is not null;"

    """
    conn = sqlite3.connect(sqlDB)
    c = conn.cursor()
    c.execute(q)
    a = c.fetchall()
    dfiles = []
    plist = []
    for row in a:
        plist.append((row[0],))
        if row[1] is not None:
            dfiles.append(row[1])
    q = 'DELETE FROM paths WHERE path = ?;'
    c.executemany(q, [plist,])
    conn.close()
    n = len(plist)
    print('Removed ' + str(n) + ' records in database')
    deleteCount = 0
    for xfn in dfiles:
        if os.path.exists(xfn):
            os.remove(xfn)
            deleteCount += 1
    print('Removed ' + str(deleteCount) + ' xml files')

def runLock(status):
    """
    runLock(status)

    Function creates a .lock file in the current directory to signify that
    xagg software is running. A lock can be place with status 'on', turned off
    with 'off', or checked using 'check'.

    Inputs:
        status: 'on' | 'off' | 'check'
    Returns:
        activeStatus: Status of runLock (True or False)

    """
    if status == 'on':
        now = datetime.datetime.now()
        fn = str(now.year) + '.' + str(now.month) + '.' + str(now.day) + '.' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second) + '.lock'
        open(fn, 'a').close()
        return True
    if status == 'off':
        l = glob.glob('*.lock')
        for f in l:
            os.remove(f)
        return False
    if status == 'check':
        if len(glob.glob('*.lock')) == 0:
            return False
        else:
            return True







