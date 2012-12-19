#!/bin/bash
DBNAME=iff

DATE=$(date +'%Y%m%d')
USERNAME=gebruikertje
PASSWORD=geheimpje
#rm /tmp/*.txt

wget --user=$USERNAME --password=$PASSWORD http://data.ndovloket.nl/ns/ns-latest.zip -O /tmp/iffns.zip
mkdir /tmp/iff/
unzip /tmp/iffns.zip -d /tmp/iff

dropdb $DBNAME
createdb $DBNAME
psql -d $DBNAME -c "create extension postgis;"
python iffparser.py /tmp/iff/
rm -rf /tmp/iff
psql -d $DBNAME -f ../sql/iff.sql

cp ../sql/stops_positioned.txt /tmp
psql -d $DBNAME -f ../sql/iff-gtfs.sql
zip -j ../gtfs/ns/gtfs-iffns-$DATE.zip /tmp/*.txt
rm ../gtfs/ns/gtfs-iffns-latest.zip
ln -s gtfs-iffns-$DATE.zip ../gtfs/ns/gtfs-iffns-latest.zip

python transitfeed/feedvalidator.py ../gtfs/ns/gtfs-iffns-$DATE.zip -o ../gtfs/ns/gtfs-iffns-$DATE.html -l 50000 
python transitfeed/kmlwriter.py ../gtfs/ns/gtfs-iffns-$DATE.zip
zip ../gtfs/ns/gtfs-iffns-$DATE.kmz ../gtfs/ns/gtfs-iffns-$DATE.kml
rm ../gtfs/ns/gtfs-iffns-$DATE.kml

