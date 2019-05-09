#!/bin/bash

# Script to pull latest CMIP table metadata

# make data directory and work from this directory
mkdir ../data
cd ../data

# clone and reorganize cmip6 tables
git clone https://github.com/PCMDI/cmip6-cmor-tables.git
mkdir cmip6
mv cmip6-cmor-tables/Tables/*.json cmip6
rm -rf cmip6-cmor-tables

# clone and reorganize cmip5 tables
git clone https://github.com/PCMDI/cmip5-cmor-tables
mkdir cmip5
mv cmip5-cmor-tables/Tables/* cmip5
rm -rf cmip5-cmor-tables

# move back to script directory
cd ../tools