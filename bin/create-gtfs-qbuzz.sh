#!/bin/bash
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DATE=$(date +'%Y%m%d')
wget ../kv1feeds/qbuzz -N --accept=zip -q -P ../kv1feeds/qbuzz -nd -r http://kv1.openov.nl/qbuzz/ -l 1
python manager.py -d kv1qbuzz -f ../kv1feeds/qbuzz
rm -rf /tmp/*.txt
psql -d kv1qbuzz -f ../sql/gtfs-shapes-qbuzz.sql
psql -d kv1qbuzz -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.zip /tmp/*.txt
rm ../gtfs/qbuzz/gtfs-kv1qbuzz-latest.zip
ln -s gtfs-kv1qbuzz-$DATE.zip ../gtfs/qbuzz/gtfs-kv1qbuzz-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.zip -o ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.zip
zip -j ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.kmz ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.kml
rm ../gtfs/qbuzz/gtfs-kv1qbuzz-$DATE.kml
