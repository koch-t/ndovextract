#!/bin/bash

psql -d kv1cxx -c "delete from timdempass"
psql -d kv1cxx -c "delete from operday"
psql -d kv1cxx -c "delete from pujopass"
psql -d kv1cxx -c "delete from schedvers"

python timdempass.py
psql -d kv1cxx -f ../sql/gtfs-shapes-connexxion.sql
DATE=$(date +'%Y%m%d')
zip -j ../gtfs/connexxion/gtfs-kv1connexxion-$DATE.zip /tmp/*.txt

rm ../gtfs/connexxion/gtfs-kv1connexxion-latest.zip
ln -s gtfs-kv1connexxion-$DATE.zip ../gtfs/connexxion/gtfs-kv1connexxion-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/connexxion/gtfs-kv1connexxion-$DATE.zip -o ../gtfs/connexxion/gtfs-kv1connexxion-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/connexxion/gtfs-kv1connexxion-$DATE.zip
