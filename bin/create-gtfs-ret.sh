#!/bin/bash

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATE=$(date +'%Y%m%d')
#wget -O /tmp/ret.zip http://kv1.openov.nl/ret-pas/ret.zip
#unzip /tmp/ret.zip -d /tmp
rm /tmp/*.txt

cp ../sql/linepublicnumbers.csv /tmp
python pasparser.py /tmp
dropdb pas
createdb pas
psql -d pas -c "Create extension postgis;"
psql -d pas -f ../sql/pas.sql
psql -d pas -f ../sql/pas-gtfs.sql
zip -j ../gtfs/ret/gtfs-pasret-$DATE.zip /tmp/*.txt
rm ../gtfs/ret/gtfs-pasret-latest.zip
ln -s gtfs-pasret-$DATE.zip ../gtfs/ret/gtfs-pasret-latest.zip

python transitfeed/feedvalidator.py ../gtfs/ret/gtfs-pasret-$DATE.zip -o ../gtfs/ret/gtfs-pasret-$DATE.html -l 50000 --error_types_ignore_list=ExpirationDate,FutureService
python transitfeed/kmlwriter.py ../gtfs/ret/gtfs-pasret-$DATE.zip
zip ../gtfs/ret/gtfs-pasret-$DATE.kmz ../gtfs/ret/gtfs-pasret-$DATE.kml
rm ../gtfs/ret/gtfs-pasret-$DATE.kml
