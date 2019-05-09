# XAGG

XML Aggregator for CMIPx data


Dependencies:

python3, joblib, cdscan (part of cdms2)

Setup:
----------------

* Create anaconda environment with dependencies (creates a Py3 environment by default)
```
> conda create -n xagg -c conda-forge -c cdat cdms2 joblib scandir
```

* Download local tables database
```
cd tools
./updateTables.sh
cd ..
```

* Execute test case
```
./xagg.py -f mon -v tas --outputDirectory /export/durack1/tmp > log.txt
```