#!/bin/bash
DATE=$(date +'%Y%m%d')
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
wget ../kv1feeds/ebs -N --accept=zip -q -P ../kv1feeds/ebs -nd -r http://kv1.openov.nl/ebs/ -l 1
python manager.py -c -d kv1ebs -f ../kv1feeds/ebs
status=$?
rm -rf /tmp/*.txt
if [ $status != 0 ];
then exit 1
fi
rm -rf /tmp/*.txt
psql -d kv1ebs -f ../sql/gtfs-shapes-ebs.sql
psql -d kv1ebs -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/ebs/gtfs-kv1ebs-$DATE.zip /tmp/agency.txt /tmp/calendar_dates.txt /tmp/feed_info.txt /tmp/routes.txt /tmp/stops.txt /tmp/stop_times.txt /tmp/trips.txt /tmp/shapes.txt
rm ../gtfs/ebs/gtfs-kv1ebs-latest.zip
ln -s gtfs-kv1ebs-$DATE.zip ../gtfs/ebs/gtfs-kv1ebs-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/ebs/gtfs-kv1ebs-$DATE.zip -o ../gtfs/ebs/gtfs-kv1ebs-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/ebs/gtfs-kv1ebs-$DATE.zip
zip -j ../gtfs/ebs/gtfs-kv1ebs-$DATE.kmz ../gtfs/ebs/gtfs-kv1ebs-$DATE.kml
rm ../gtfs/ebs/gtfs-kv1ebs-$DATE.kml
