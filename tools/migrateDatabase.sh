#!/bin/bash

# Script to migrate sqlite file to MySQL

echo "Export sqlite3 database"
sqlite3 ../xml.db .dump | python sqlite3-to-mysql.py > mysql.sql
sed -i '1d' mysql.sql
sed -i '1 s/^/DROP TABLE IF EXISTS paths; DROP TABLE IF EXISTS invalid_paths;  DROP TABLE IF EXISTS runs; DROP TABLE IF EXISTS stats;\n/' mysql.sql
sed -i '1 s/^/SET autocommit=0;\n/' mysql.sql
echo 'COMMIT;' >> mysql.sql
echo "Import database"
mysql -u pochedls -p xagg < mysql.sql
rm mysql.sql
