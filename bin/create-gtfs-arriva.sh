#!/bin/bash
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=$(date +'%Y%m%d')
USERNAME=gebruikersnaam
PASSWORD=wachtwoord
wget --user $USERNAME --password $PASSWORD -N --accept=zip  -P ../kv1feeds/arriva -nd -r http://data.ndovloket.nl/arr/ -l 1
python manager.py -x -n -c -d kv1arr -f ../kv1feeds/arriva
status=$?
rm -rf /tmp/*.txt
if [ $status != 0 ];
then exit 1
fi
rm -rf /tmp/*.txt
psql -d kv1arr -f ../sql/gtfs-shapes-arriva.sql
psql -d kv1arr -f ../sql/gtfs-shapes-passtimes.sql
zip -j ../gtfs/arriva/gtfs-kv1arriva-$DATE.zip /tmp/agency.txt /tmp/calendar_dates.txt /tmp/feed_info.txt /tmp/routes.txt /tmp/stops.txt /tmp/stop_times.txt /tmp/trips.txt /tmp/shapes.txt
rm ../gtfs/arriva/gtfs-kv1arriva-latest.zip
ln -s gtfs-kv1arriva-$DATE.zip ../gtfs/arriva/gtfs-kv1arriva-latest.zip

#Validate using Google TransitFeed
python transitfeed/feedvalidator.py ../gtfs/arriva/gtfs-kv1arriva-$DATE.zip -o ../gtfs/arriva/gtfs-kv1arriva-$DATE.html -n -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/arriva/gtfs-kv1arriva-$DATE.zip
zip -j ../gtfs/arriva/gtfs-kv1arriva-$DATE.kmz ../gtfs/arriva/gtfs-kv1arriva-$DATE.kml
rm ../gtfs/arriva/gtfs-kv1arriva-$DATE.kml
