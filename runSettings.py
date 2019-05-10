# file contains standard xagg run information

sqlDB = 'xml.db' # sqlite database path
cmipMetaFile = 'data/cmipMeta.pkl' # CMIP metadata pickle file
chunkSize = 1000 # Chunk size (number of directories to scan in a given parallelization)

# parent directories to scan
data_directories = ['/p/css03/cmip5_css01/data/cmip5/output1/',
                    '/p/css03/cmip5_css01/data/cmip5/output2/',
                    '/p/css03/cmip5_css02/data/cmip5/output1/',
                    '/p/css03/cmip5_css02/data/cmip5/output2/',
                    '/p/css03/esgf_publish/cmip5/',
                    '/p/css03/scratch/cmip5/',
                    '/p/css03/scratch/obsolete/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/',
                    '/p/css03/scratch/published-latest/cmip5/cmip5_css01/scratch/cmip5/',
                    '/p/css03/scratch/published-older/cmip5/',
                    '/p/css03/scratch/should-publish/cmip5/',
                    '/p/css03/scratch/unknown-dset/cmip5/',
                    '/p/css03/scratch/unknown-status/cmip5/',
                    '/p/user_pub/xclim/extension/']
#data_directories = ['/p/css03/scratch/cmip6/DAMIP/'] ; # Test

# parent directories to scan (split these up for parallelization)
split_directories = ['/p/css03/esgf_publish/CMIP6/', '/p/css03/scratch/cmip6/']
#split_directories = [] ; # Test

# directories to ignore
rm_directories = ['/p/css03/esgf_publish/CMIP6/input4MIPs/']
#rm_directories = [] ; # Test

# variables to scan
variables = ['agessc','areacella','areacello','basin','cfc11','cl','clcalipso',
             'clhcalipso','cli','clisccp','clivi','cllcalipso','clmcalipso','clt',
             'cltcalipso','clw','clwvi','cropfrac','deptho','dissic','evs','evspsbl',
             'evspsblsoi','evspsblveg','ficeberg','friver','gpp','hfds','hfls',
             'hfls','hfss','hur','hurs','hus','huss','lai','mc','mfo','mlotst',
             'mrfso','mrro','mrros','mrso','mrsofc','mrsos','omlmax','orog','pflw',
             'pfull','ph','pr','prc','prsn','prw','ps','psl','rhopoto','rlds','rldscs',
             'rlus','rluscs','rlut','rlutcs','rsds','rsdscs','rsdt','rsus','rsuscs',
             'rsut','rsutcs','sbl','sci','sfcWind','sfriver','sftgif','sftlf','sftof',
             'sic','sim','sit','snc','snd','snw','so','soga','sos','ta','tas','tasmax',
             'tasmin','tauu','tauuo','tauv','tauvo','thetao','thetaoga','tos','tpf',
             'tran','ts','tsl','ua','uas','uo','va','vas','vo','volcello','vsf',
             'vsfcorr','vsfevap','vsfpr','vsfriver','wap','wfo','wfonocorr','zg',
             'zos','zostoga']

# frequencies to scan
frequencies = ['fx','mon']

# experiments to scan
experiments = ['1pctCO2','abrupt-0p5xCO2','abrupt-2xCO2','abrupt-4xCO2',
               'abrupt-solm4p','abrupt-solp4p','abrupt4xCO2','amip','amip-4xCO2',
               'amip-m4K','amip-p4K','amip-piForcing','amip4K','amip4xCO2',
               'amipFuture','hist-GHG','hist-aer','hist-nat','historical',
               'historicalExt','historicalGHG','historicalMisc','historicalNat',
               'past1000','piClim-4xCO2','piClim-aerO3','piClim-anthro',
               'piClim-control','piClim-ghg','piClim-histaerO3','piClim-histall',
               'piClim-histghg','piClim-histnat','piClim-lu','piControl','rcp26',
               'rcp45','rcp60','rcp85','ssp119','ssp126','ssp245','ssp370',
               'ssp434','ssp460','ssp585amip-future4K','sstClim','sstClim4xCO2']
#experiments = ['historical'] ; # Test