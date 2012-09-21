#!/bin/bash
DATE=$(date +'%Y%m%d')
wget ../kv1feeds/ebs -N --accept=zip -q -P ../kv1feeds/ebs -nd -r http://kv1.openov.nl/ebs/ -l 1
python manager.py -d kv1ebs -f ../kv1feeds/ebs
rm -rf /tmp/*.txt
psql -d kv1ebs -f ../sql/gtfs-shapes-ebs.sql
psql -d kv1ebs -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/ebs/gtfs-kv1ebs-$DATE.zip /tmp/*.txt
rm ../gtfs/ebs/gtfs-kv1ebs-latest.zip
ln -s gtfs-kv1ebs-$DATE.zip ../gtfs/ebs/gtfs-kv1ebs-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/ebs/gtfs-kv1ebs-$DATE.zip -o ../gtfs/ebs/gtfs-kv1ebs-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/ebs/gtfs-kv1ebs-$DATE.zip
zip -j ../gtfs/ebs/gtfs-kv1ebs-$DATE.kmz ../gtfs/ebs/gtfs-kv1ebs-$DATE.kml
rm ../gtfs/ebs/gtfs-kv1ebs-$DATE.kml
