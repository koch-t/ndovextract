#!/bin/bash
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=$(date +'%Y%m%d')
wget ../kv1feeds/htm -N --accept=zip  -P ../kv1feeds/htm -nd -r http://kv1.openov.nl/htm/ -l 1
python manager.py -c -d kv1htm -f ../kv1feeds/htm
status=$?
rm -rf /tmp/*.txt
if [ $status != 0 ];
then exit 1
fi
rm -rf /tmp/*.txt
psql -d kv1htm -f ../sql/gtfs-shapes-htm.sql
psql -d kv1htm -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/htm/gtfs-kv1htm-$DATE.zip /tmp/agency.txt /tmp/calendar_dates.txt /tmp/feed_info.txt /tmp/routes.txt /tmp/stops.txt /tmp/stop_times.txt /tmp/trips.txt /tmp/shapes.txt
rm ../gtfs/htm/gtfs-kv1htm-latest.zip
ln -s gtfs-kv1htm-$DATE.zip ../gtfs/htm/gtfs-kv1htm-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator_googletransit.py ../gtfs/htm/gtfs-kv1htm-$DATE.zip -o ../gtfs/htm/gtfs-kv1htm-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/htm/gtfs-kv1htm-$DATE.zip
zip ../gtfs/htm/gtfs-kv1htm-$DATE.kmz ../gtfs/htm/gtfs-kv1htm-$DATE.kml
rm ../gtfs/htm/gtfs-kv1htm-$DATE.kml
