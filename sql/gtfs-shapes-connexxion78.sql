-- GTFS: feed_info.txt
COPY (
SELECT
'OVapi' as feed_publisher_name,
'http://ovapi.nl/' as feed_publisher_url,
'nl' as feed_lang,
replace(cast(min(validfrom) AS text), '-', '') as feed_start_date,
replace(cast(max(validthru) AS text), '-', '') as feed_end_date,
now() as feed_version 
FROM 
pegrval
) TO '/tmp/feed_info.txt' WITH CSV HEADER;

COPY (
SELECT
'CXX' as agency_id,
'Connexxion' as agency_name,
'http://www.connexxion.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
UNION
SELECT
'BRENG' as agency_id,
'Breng' as agency_name,
'http://www.breng.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
UNION
SELECT
'HERMES' as agency_id,
'Hermes' as agency_name,
'http://www.hermes.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
UNION
SELECT
'GVU' as agency_id,
'GVU' as agency_name,
'http://www.gvu.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
) TO '/tmp/agency.txt' WITH CSV HEADER;

-- GTFS: shapes.txt
-- -- Missing:
-- KV1 support for LinkValidFrom
-- GTFS support for shape_dist_traveled (summation of distancesincestartoflink)
-- ** disabled transporttype **
COPY (
SELECT DISTINCT shape_id,
      CAST(ST_Y(the_geom) AS NUMERIC(8,5)) AS shape_pt_lat,
      CAST(ST_X(the_geom) AS NUMERIC(7,5)) AS shape_pt_lon,
      shape_pt_sequence
FROM
 (SELECT DISTINCT ON (
           jopatili.version,
           jopatili.dataownercode,
           jopatili.lineplanningnumber,
           jopatili.journeypatterncode,
           jopatili.timinglinkorder,
           pool.distancesincestartoflink,
           pool.linkvalidfrom) 
  jopatili.version||'|'||jopatili.dataownercode||'|'||jopatili.lineplanningnumber||'|'||jopatili.journeypatterncode AS shape_id,
  ST_Transform(st_setsrid(st_makepoint(locationx_ew, locationy_ns), 28992), 4326) AS the_geom,
  rank() over (PARTITION BY jopatili.version,jopatili.dataownercode, jopatili.lineplanningnumber, jopatili.journeypatterncode ORDER BY
jopatili.version,jopatili.dataownercode, jopatili.lineplanningnumber, jopatili.journeypatterncode, jopatili.timinglinkorder,
pool.distancesincestartoflink) AS shape_pt_sequence
  FROM jopatili,
        pool,
       point,
       line
  WHERE jopatili.dataownercode = pool.dataownercode
    AND jopatili.userstopcodebegin = pool.userstopcodebegin
    AND jopatili.userstopcodeend = pool.userstopcodeend
    AND jopatili.version = pool.version
    AND jopatili.dataownercode = line.dataownercode
    AND jopatili.lineplanningnumber = line.lineplanningnumber
    and jopatili.version = line.version
    AND pool.pointdataownercode = point.dataownercode
    AND pool.pointcode = point.pointcode
    AND pool.version = point.version
--    AND pool.transporttype = line.transporttype
    AND jopatili.lineplanningnumber not in (select lineplanningnumber from line78)
  ORDER BY
           jopatili.version,
           jopatili.dataownercode,
           jopatili.lineplanningnumber,
           jopatili.journeypatterncode,
           jopatili.timinglinkorder,
           pool.distancesincestartoflink,
           pool.linkvalidfrom) AS KV1
) TO '/tmp/shapes.txt' WITH CSV HEADER;

COPY (
SELECT stop_id || '|parent' as stop_id, a.name AS stop_name,
       CAST(ST_Y(the_geom) AS NUMERIC(8,5)) AS stop_lat,
       CAST(ST_X(the_geom) AS NUMERIC(7,5)) AS stop_lon,
       1 AS location_type,
       NULL AS parent_station
FROM (SELECT parent_station AS stop_id,
               ST_Transform(ST_setsrid(ST_makepoint(AVG(locationx_ew), AVG(locationy_ns)), 28992), 4326) AS the_geom,
               version
        FROM (SELECT u.dataownercode || '|' || u.userstopareacode AS parent_station,
                       locationx_ew,
                       locationy_ns,
                       u.version
                FROM usrstop AS u,
                       point AS p
                WHERE u.dataownercode = p.dataownercode AND
                       u.version = p.version AND
                       u.userstopcode = p.pointcode AND
                       u.userstopareacode IS NOT NULL) AS x
        GROUP BY version,parent_station) AS y,
        (SELECT DISTINCT ON (dataownercode,userstopareacode) * FROM usrstar ORDER BY dataownercode,userstopareacode,version DESC) AS a
WHERE
stop_id = a.dataownercode || '|' || a.userstopareacode AND
a.version = y.version
UNION
SELECT stop_id,
       stop_name,
       CAST(ST_Y(the_geom) AS NUMERIC(8,5)) AS stop_lat,
       CAST(ST_X(the_geom) AS NUMERIC(7,5)) AS stop_lon,
       location_type,
       parent_station
FROM (SELECT u.dataownercode||'|'||u.userstopcode AS stop_id,
               u.name AS stop_name,
               ST_Transform(ST_setsrid(ST_makepoint(p.locationx_ew, p.locationy_ns), 28992), 4326) AS the_geom,
               0 AS location_type,
               u.dataownercode||'|'||u.userstopareacode||'|parent' AS parent_station
        FROM (SELECT DISTINCT ON (dataownercode,userstopcode) * FROM usrstop ORDER BY dataownercode,userstopcode,version DESC)AS u,
              point AS p
        WHERE u.dataownercode = p.dataownercode AND
               u.userstopcode = p.pointcode AND
               u.version = p.version AND
               (u.getin = TRUE OR u.getout = TRUE) AND
                u.userstopcode IN (SELECT userstopcodebegin FROM jopatili 
                                   UNION SELECT userstopcodeend FROM jopatili)) AS KV1
) TO '/tmp/stops.txt' WITH CSV HEADER;

DROP TABLE gtfs_route_type;
CREATE TABLE gtfs_route_type (transporttype varchar(5) primary key, route_type int4);
INSERT INTO gtfs_route_type VALUES ('TRAM', 0);
INSERT INTO gtfs_route_type VALUES ('METRO', 1);
INSERT INTO gtfs_route_type VALUES ('TRAIN', 2);
INSERT INTO gtfs_route_type VALUES ('BUS', 3);
INSERT INTO gtfs_route_type VALUES ('BOAT', 4);

create table gtfs_wheelchair_accessibility (wheelchairaccessible varchar(13) primary key, wheelchair_accessible int4);
insert into gtfs_wheelchair_accessibility values ('UNKNOWN', 0);
insert into gtfs_wheelchair_accessibility values ('ACCESSIBLE', 1);
insert into gtfs_wheelchair_accessibility values ('NOTACCESSIBLE', 2);

drop table productformula;
CREATE TABLE productformula (productformulatype decimal(4) primary key, localized_route_type varchar(20));
insert into productformula VALUES(1,'Buurtbus');
insert into productformula VALUES(2,'Belbus');
insert into productformula VALUES(3,'Express-bus');
insert into productformula VALUES(4,'Fast Ferry');
insert into productformula VALUES(5,'Hanze-Liner');
insert into productformula VALUES(6,'Interliner');
insert into productformula VALUES(7,'Kamperstadslijn');
insert into productformula VALUES(8,'Lijntaxi');
insert into productformula VALUES(9,'Media express');
insert into productformula VALUES(10,'MAXX');
insert into productformula VALUES(11,'Natuurexpress');
insert into productformula VALUES(12,'Niteliner');
insert into productformula VALUES(13,'Q-liner');
insert into productformula VALUES(14,'Regioliner');
insert into productformula VALUES(15,'Servicebus');
insert into productformula VALUES(16,'Sneldienst');
insert into productformula VALUES(17,'Spitsbus');
insert into productformula VALUES(19,'Sternet');
insert into productformula VALUES(20,'Sneltram');
insert into productformula VALUES(21,'Tram');
insert into productformula VALUES(22,'Vierdaagse');
insert into productformula VALUES(23,'Waterbus');
insert into productformula VALUES(24,'Zuidtangent');
insert into productformula VALUES(25,'Stoptrein');
insert into productformula VALUES(26,'Sneltrein');
insert into productformula VALUES(27,'Intercity');
insert into productformula VALUES(28,'Sprinter');
insert into productformula VALUES(29,'Internationale Trein');
insert into productformula VALUES(30,'HSL Zuid');
insert into productformula VALUES(31,'ICE');
insert into productformula VALUES(32,'Thalys');
insert into productformula VALUES(33,'Valleilijn');
insert into productformula VALUES(34,'Breng');
insert into productformula VALUES(35,'Opstapper');
insert into productformula VALUES(36,'Overstapper');
insert into productformula VALUES(37,'R-NET');
insert into productformula VALUES(38,'Parkshuttle');
insert into productformula VALUES(39,'FC-Utrecht Express');

COPY (
SELECT DISTINCT ON (line.dataownercode,line.lineplanningnumber)
line.dataownercode||'|'||line.lineplanningnumber AS route_id,
CASE
     WHEN (substring(line.lineplanningnumber,1,1) = 'U')          THEN 'GVU'
     WHEN  (substring(line.lineplanningnumber,1,1) IN ('A','X'))    THEN 'BRENG'
     WHEN (substring(line.lineplanningnumber,1,1) = 'L')           THEN 'HERMES'
     ELSE line.dataownercode END AS agency_id,
linepublicnumber AS route_short_name,
linename AS route_long_name,
route_type AS route_type,
localized_route_type
FROM gtfs_route_type,line,
-- Could cause some wrong productformula as we're looking at the whole line and productformula can differ per journeypttern
(select distinct version,dataownercode,lineplanningnumber,productformulatype from jopatili) as pf LEFT JOIN productformula USING
(productformulatype)
WHERE
coalesce(line.transporttype,'BUS') = gtfs_route_type.transporttype AND
line.transporttype != 'TRAIN' AND
pf.version = line.version AND
pf.dataownercode = line.dataownercode AND
pf.lineplanningnumber = line.lineplanningnumber
ORDER BY line.dataownercode,line.lineplanningnumber,line.version DESC
) TO '/tmp/routes.txt' WITH CSV HEADER;

COPY (
SELECT
service_id,
replace(cast(date as text), '-', '') as date,
1 as exception_type
FROM(
	SELECT
	service_id,
	CASE WHEN (validfrom != validthru) THEN cast (generate_series(validfrom,validthru,'1 day') as date) ELSE validfrom END as date,
        daytype,
        version
	FROM (
		SELECT
                pj.version,
		pj.version||'|'||pj.dataownercode||'|'||pj.timetableversioncode||'|'||pj.organizationalunitcode||'|'||pj.periodgroupcode||'|'||pj.specificdaycode||'|'||pj.daytype AS service_id,
		pj.daytype,
		pv.validfrom as validfrom,
		coalesce(tv.validthru,pv.validthru) as validthru
		from pegrval as pv, pujo as pj, tive as tv
		where
		pv.dataownercode = tv.dataownercode and
		pv.organizationalunitcode = tv.organizationalunitcode and
		pv.periodgroupcode = tv.periodgroupcode AND
		pv.version = tv.version AND
		tv.version = pj.version AND
		tv.dataownercode = pj.dataownercode AND
		tv.timetableversioncode = pj.timetableversioncode AND
		tv.periodgroupcode = pj.periodgroupcode AND
                pv.organizationalunitcode not in (select lineplanningnumber from line78) AND
		tv.specificdaycode = pj.specificdaycode) as calendar
	) as calendar_dates,version as v
WHERE
position( CAST(CASE WHEN extract(dow from date) = 0 THEN 7 ELSE extract(dow from date) END as text) in daytype) != 0 AND
v.version = calendar_dates.version AND date between v.validfrom and v.validthru
AND NOT EXISTS (
  SELECT 1 FROM (select cast(validdate as date) as excopdate,left(cast(daytypeason as text),1) as daytypeason from excopday) as excopday
  WHERE date = excopdate and position( CAST(CASE WHEN extract(dow from date) = 0 THEN 7 ELSE extract(dow from date) END as text) in daytypeason) = 0
  ) AND
date >= date 'yesterday'
UNION
SELECT
x.version||'|'||v.dataownercode||'|'||timetableversioncode||'|'||organizationalunitcode||'|'||periodgroupcode||'|'||specificdaycode||'|'||daytype AS service_id,
replace(cast(date as text), '-', '') as date,
1 as exception_type
FROM(
	SELECT
	*,
        (select distinct daytype 
         FROM pujo
         WHERE pujo.version = dates.version AND pujo.dataownercode = dates.dataownercode AND pujo.timetableversioncode = dates.timetableversioncode 
                AND pujo.organizationalunitcode = dates.organizationalunitcode AND pujo.periodgroupcode = dates.periodgroupcode 
                AND pujo.specificdaycode = dates.specificdaycode AND position( daytypeason in daytype) != 0)
	FROM(
		SELECT
		*,
		CASE WHEN (validfrom != validthru) THEN cast (generate_series(validfrom,validthru,'1 day') as date) ELSE validfrom END as date
		FROM (
			SELECT
			pj.version,
		        pj.dataownercode,
		        pj.timetableversioncode,
		        pj.organizationalunitcode,
		        pj.periodgroupcode,
		        pj.specificdaycode,
			pv.validfrom as validfrom,
			coalesce(tv.validthru,pv.validthru) as validthru
			from pegrval as pv, pujo as pj, tive as tv
			where
			pv.dataownercode = tv.dataownercode and
			pv.organizationalunitcode = tv.organizationalunitcode and
                        pv.organizationalunitcode not in (select lineplanningnumber from line78) AND
			pv.periodgroupcode = tv.periodgroupcode AND
			pv.version = tv.version AND
			tv.version = pj.version AND
			tv.dataownercode = pj.dataownercode AND
			tv.timetableversioncode = pj.timetableversioncode AND
			tv.periodgroupcode = pj.periodgroupcode AND
			tv.specificdaycode = pj.specificdaycode) as calendar
		) as dates,
                 (select cast(validdate as date) as excopdate,left(cast(daytypeason as text),1) as daytypeason from excopday) as excopday
	WHERE
                excopdate = date) as x,version as v
WHERE
daytype is not null AND
date >= date 'yesterday' AND
x.version = v.version AND
v.version = x.version AND date between v.validfrom and v.validthru
) TO '/tmp/calendar_dates.txt' WITH CSV HEADER;

-- GTFS: trips.txt (Geldigheden en rijtijdgroepen)
--
-- Missing:
-- KV1 doesn't disclose information about block_id (same busses used for the next trip)
COPY (
SELECT
 p.dataownercode||'|'||p.lineplanningnumber AS route_id,
 p.version||'|'||p.dataownercode||'|'||p.timetableversioncode||'|'||p.organizationalunitcode||'|'||p.periodgroupcode||'|'||p.specificdaycode||'|'||p.daytype AS service_id,
 p.version||'|'||p.dataownercode||'|'||p.periodgroupcode||'|'||p.daytype||'|'||p.lineplanningnumber||'|'||p.journeynumber AS trip_id,
 d.destnamefull AS trip_headsign,
 (CAST(j.direction AS int4) - 1) AS direction_id,
 jt.version||'|'||jt.dataownercode||'|'||jt.lineplanningnumber||'|'||jt.journeypatterncode AS shape_id
FROM pujo AS p, jopa AS j, dest AS d, gtfs_wheelchair_accessibility as g,
( SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
  version,dataownercode,lineplanningnumber,journeypatterncode,userstopcodebegin,destcode
  FROM jopatili
  ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder
 ) as jt
WHERE
 p.dataownercode = j.dataownercode AND
 p.lineplanningnumber = j.lineplanningnumber AND
 p.journeypatterncode = j.journeypatterncode AND
 p.version = j.version AND
 j.dataownercode = jt.dataownercode AND
 j.lineplanningnumber = jt.lineplanningnumber AND
 j.journeypatterncode = jt.journeypatterncode AND
 j.version = jt.version AND
 jt.dataownercode = d.dataownercode AND
 jt.destcode = d.destcode AND
 jt.version = d.version AND
 p.wheelchairaccessible = g.wheelchairaccessible AND
 p.lineplanningnumber not in (select lineplanningnumber from line78) AND
 p.dataownerisoperator = true
) TO '/tmp/trips.txt' WITH CSV HEADER;

COPY (
SELECT
p.version||'|'||p.dataownercode||'|'||p.periodgroupcode||'|'||p.daytype||'|'||p.lineplanningnumber||'|'||p.journeynumber AS trip_id,
1 as stop_sequence,
p.dataownercode||'|'||userstopcodebegin as stop_id,
departuretime as arrival_time,
departuretime as departure_time,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getin as integer) END as pickup_type,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getout as integer) END as drop_off_type
FROM
pujo as p, usrstop as u,
( SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
  version,dataownercode,lineplanningnumber,journeypatterncode,userstopcodebegin,productformulatype
  FROM jopatili
  ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder
 ) as j
WHERE 
p.version = j.version AND
p.dataownercode = j.dataownercode AND
p.lineplanningnumber = j.lineplanningnumber AND
p.journeypatterncode = j.journeypatterncode AND
p.version = u.version AND
userstopcodebegin = u.userstopcode AND
u.userstoptype = 'PASSENGER' AND
p.lineplanningnumber not in (select lineplanningnumber from line78) AND
p.dataownerisoperator = true
UNION
SELECT
 p.version||'|'||p.dataownercode||'|'||p.periodgroupcode||'|'||p.daytype||'|'||p.lineplanningnumber||'|'||p.journeynumber AS trip_id,
(t.timinglinkorder + 2) as stop_sequence,
p.dataownercode||'|'||t.userstopcodeend as stop_id,
add32time(departuretime,totaldrivetime) as arrival_time,
add32time(departuretime,cast((totaldrivetime+stopwaittime)as integer)) as departure_time,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getin as integer) END as pickup_type,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getout as integer) END as drop_off_type 
FROM
pujo as p, timdempass as t, usrstop as u,jopatili as j
WHERE
p.version = t.version AND
p.dataownercode = t.dataownercode AND
p.lineplanningnumber = t.lineplanningnumber AND
p.journeypatterncode = t.journeypatterncode AND
p.lineplanningnumber not in (select lineplanningnumber from line78) AND
p.timedemandgroupcode = t.timedemandgroupcode AND
p.version = u.version AND
t.userstopcodeend = u.userstopcode AND
u.userstoptype = 'PASSENGER' AND
p.version = j.version AND
p.dataownercode = j.dataownercode AND
p.lineplanningnumber = j.lineplanningnumber AND
p.journeypatterncode = j.journeypatterncode AND
t.timinglinkorder = j.timinglinkorder AND
p.dataownerisoperator = true
) TO '/tmp/stop_times.txt' WITH CSV HEADER;
