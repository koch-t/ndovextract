#!/bin/bash
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DATE=$(date +'%Y%m%d')
wget ../kv1feeds/syntus -N --accept=zip -q -P ../kv1feeds/syntus -nd -r http://kv1.openov.nl/syntus/ -l 1
python manager.py -d kv1syntus -f ../kv1feeds/syntus
psql -d kv1syntus -f ../sql/gtfs-shapes-syntus.sql
psql -d kv1syntus -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/syntus/gtfs-kv1syntus-$DATE.zip /tmp/*.txt
rm ../gtfs/syntus/gtfs-kv1syntus-latest.zip
ln -s gtfs-kv1syntus-$DATE.zip ../gtfs/syntus/gtfs-kv1syntus-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/syntus/gtfs-kv1syntus-$DATE.zip -o ../gtfs/syntus/gtfs-kv1syntus-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/syntus/gtfs-kv1syntus-$DATE.zip
zip ../gtfs/syntus/gtfs-kv1syntus-$DATE.kmz ../gtfs/syntus/gtfs-kv1syntus-$DATE.kml
rm ../gtfs/syntus/gtfs-kv1syntus-$DATE.kml
