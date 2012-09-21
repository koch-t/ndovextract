#!/bin/bash
DATE=$(date +'%Y%m%d')
wget ../kv1feeds/htm -N --accept=zip -q -P ../kv1feeds/htm -nd -r http://kv1.openov.nl/htm/ -l 1
python manager.py -d kv1htm -f ../kv1feeds/htm
rm -rf /tmp/*.txt
psql -d kv1htm -f ../sql/gtfs-shapes-htm.sql
psql -d kv1htm -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/htm/gtfs-kv1htm-$DATE.zip /tmp/*.txt
rm ../gtfs/htm/gtfs-kv1htm-latest.zip
ln -s gtfs-kv1htm-$DATE.zip ../gtfs/htm/gtfs-kv1htm-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/htm/gtfs-kv1htm-$DATE.zip -o ../gtfs/htm/gtfs-kv1htm-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/htm/gtfs-kv1htm-$DATE.zip
zip ../gtfs/htm/gtfs-kv1htm-$DATE.kmz ../gtfs/htm/gtfs-kv1htm-$DATE.kml
rm ../gtfs/htm/gtfs-kv1htm-$DATE.kml
