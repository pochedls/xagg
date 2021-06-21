# XAGG

XML Aggregator for CMIPx data

Dependencies:
```
python3, scipy, joblib, scandir, cdscan (part of cdms2)
```

Setup:
----------------
* Create anaconda environment with dependencies (creates a Py3 environment by default)
```
mamba create -y -n cdat -c conda-forge -c cdat/label/v8.2.1 "libnetcdf=*=mpi_openmpi_*" "mesalib=18.3.1" "python=3.7" cdat cdms2 joblib scandir scipy
```

* Download local tables database
```
cd tools
./updateTables.sh
cd ..
```

Run software:
----------------
* Execute test cases
```
./xagg.py -f mon -v tas --outputDirectory ~/tmp > log.txt
```
Or
```
./xagg.py -f mon -v tas -e hist-CO2 --outputDirectory ~/tmp --updatePaths False > log.txt
```

* Execute complete scan
```
./xagg.py --outputDirectory ~/tmp > log.txt
```
