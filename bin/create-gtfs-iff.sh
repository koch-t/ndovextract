#!/bin/bash
DBNAME=iff

DATE=$(date +'%Y%m%d')
USERNAME=password
PASSWORD=username
#rm /tmp/*.txt

wget --user=$USERNAME --password=$PASSWORD http://data.ndovloket.nl/ns/ns-latest.zip -O /tmp/iffns.zip
mkdir /tmp/iff/
unzip /tmp/iffns.zip -d /tmp/iff

dropdb $DBNAME
createdb $DBNAME
psql -d $DBNAME -c "create extension postgis;"
python iffparser.py /tmp/iff/
status=$?
rm -rf /tmp/*.txt
if [ $status != 0 ];
then exit 1
fi
rm -rf /tmp/iff
psql -d $DBNAME -f ../sql/iff.sql

cp ../sql/stops_positioned.txt /tmp
psql -d $DBNAME -f ../sql/iff-gtfs.sql
zip -j ../gtfs/ns/gtfs-iffns-$DATE.zip /tmp/agency.txt /tmp/calendar_dates.txt /tmp/feed_info.txt /tmp/routes.txt /tmp/stops.txt /tmp/stop_times.txt /tmp/trips.txt /tmp/transfers.txt
rm ../gtfs/ns/gtfs-iffns-latest.zip
ln -s gtfs-iffns-$DATE.zip ../gtfs/ns/gtfs-iffns-latest.zip

python transitfeed/feedvalidator.py -n ../gtfs/ns/gtfs-iffns-$DATE.zip -o ../gtfs/ns/gtfs-iffns-$DATE.html -l 50000 
python transitfeed/kmlwriter.py ../gtfs/ns/gtfs-iffns-$DATE.zip
zip ../gtfs/ns/gtfs-iffns-$DATE.kmz ../gtfs/ns/gtfs-iffns-$DATE.kml
psql -d $DBNAME -c "copy (select null as from_stop_id,null as to_stop_id,0 as transfer_type, 0 as min_transfer_time limit 0) to '/tmp/transfers.txt' CSV HEADER"
rm ../gtfs/ns/gtfs-iffns-$DATE.kml

