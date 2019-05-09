# file contains standard xagg run information

sqlDB = 'xml.db' # sqlite database path

cmipMetaFile = 'data/cmipMeta.pkl' # CMIP metadata pickle file

chunkSize = 1000 # Chunk size (number of directories to scan in a given parallelization)

# parent directories to check
data_directories = ['/p/css03/cmip5_css01/data/cmip5/output1/', '/p/css03/cmip5_css01/data/cmip5/output2/',
                    '/p/css03/cmip5_css02/data/cmip5/output1/', '/p/css03/cmip5_css02/data/cmip5/output2/', 
                    '/p/css03/scratch/cmip5/', '/p/css03/scratch/published-latest/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/',
                    '/p/css03/scratch/published-older/cmip5/', '/p/css03/scratch/should-publish/cmip5/',
                    '/p/css03/scratch/unknown-dset/cmip5/', '/p/css03/scratch/unknown-status/cmip5/',
                    '/p/css03/scratch/obsolete/cmip5/', '/p/css03/esgf_publish/cmip5/', '/p/user_pub/xclim/extension/']

# parent directories to check (split these up for parallelization)
split_directories = ['/p/css03/esgf_publish/CMIP6/', '/p/css03/scratch/cmip6/']

# directories to ignore
rm_directories = ['/p/css03/esgf_publish/CMIP6/input4MIPs/']

# variables to scan
variables = ['snc','snd','snw','tpf','pflw', 'sic','sim','sit','snc','snd', 'agessc','cfc11','dissic','evs','ficeberg',\
    'friver','hfds','hfls','hfss','mfo','mlotst','omlmax','ph','pr','rlds', 'rhopoto','rsds','sfriver','so','soga',\
    'sos','tauuo','tauvo','thetao','thetaoga','tos','uo','vo','vsf','vsfcorr', 'vsfevap','vsfpr','vsfriver','wfo',\
    'wfonocorr','zos','zostoga', 'cropfrac','evspsblsoi','evspsblveg','gpp','lai','mrfso','mrro','mrros','mrso',\
    'mrsos','tran','tsl', 'areacella','areacello','basin','deptho','mrsofc','orog','sftgif','sftlf','sftof','volcello', \
    'cl','clcalipso','cli','clisccp','clivi','clt','clw','clwvi','evspsbl','hfls','hfss','hur','hurs', 'hus','huss',\
    'mc','pr','prc','prsn','prw','ps','psl','rlds','rldscs','rlus','rluscs','rlut', 'rlutcs','rsds','rsdscs','rsdt',\
    'rsus','rsuscs','rsut','rsutcs','sbl','sci','sfcWind', 'ta','tas','tasmax','tasmin','tauu','tauv','ts','ua','uas',\
    'va','vas','wap','zg', 'clhcalipso', 'clmcalipso', 'cllcalipso', 'cltcalipso', 'pfull']   

# frequencies to scan
frequencies = ['fx','mon']

# experiments to scan
experiments = ['1pctCO2','abrupt4xCO2','amip','amip4K','amip4xCO2','amipFuture','historical','historicalExt', \
        'historicalGHG','historicalMisc','historicalNat','past1000','piControl','rcp26','rcp45','rcp60',\
        'rcp85', 'sstClim','sstClim4xCO2','abrupt-4xCO2', 'amip-4xCO2', 'amip-p4K', 'amip-m4K', 'hist-aer',\
        'hist-GHG', 'hist-nat', 'ssp119', 'ssp126', 'ssp245', 'ssp370', 'ssp434', 'ssp460', 'ssp585'\
        'amip-future4K', 'abrupt-solp4p', 'abrupt-solm4p', 'abrupt-2xCO2', 'abrupt-0p5xCO2', 'piClim-control',\
        'piClim-anthro', 'piClim-ghg', 'piClim-aerO3', 'piClim-lu', 'piClim-4xCO2', 'piClim-histall',\
        'piClim-histnat', 'piClim-histghg', 'piClim-histaerO3', 'amip-piForcing']

