#!/bin/bash
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=$(date +'%Y%m%d')
python manager.py -d kv1arr -f ../kv1feeds/arriva
rm -rf /tmp/*.txt
psql -d kv1arr -f ../sql/gtfs-shapes-arriva.sql
psql -d kv1arr -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/arriva/gtfs-kv1arriva-$DATE.zip /tmp/*.txt
rm ../gtfs/arriva/gtfs-kv1arriva-latest.zip
ln -s gtfs-kv1arriva-$DATE.zip ../gtfs/arriva/gtfs-kv1arriva-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/arriva/gtfs-kv1arriva-$DATE.zip -o ../gtfs/arriva/gtfs-kv1arriva-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/arriva/gtfs-kv1arriva-$DATE.zip
zip ../gtfs/arriva/gtfs-kv1arriva-$DATE.kmz ../gtfs/arriva/gtfs-kv1arriva-$DATE.kml
rm ../gtfs/arriva/gtfs-kv1arriva-$DATE.kml
