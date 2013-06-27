DELETE FROM pujopass WHERE                                                                                            
concat_ws('|',version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode) not in 
(SELECT distinct concat_ws('|',version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode) from operday);;

DELETE FROM schedpujo WHERE
concat_ws('|',version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode) not in
(SELECT distinct concat_ws('|',version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode) from operday);;

DELETE FROM schedvers WHERE                                                                                            
concat_ws('|',version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode) not in 
(SELECT distinct concat_ws('|',version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode) from operday);;

--Some old prodform modifications
update jopatili set productformulatype = 1 where dataownercode = 'VTN' and deprecated = 'buur';
update jopatili set productformulatype = 2 where dataownercode = 'VTN' and deprecated = 'belb';
update jopatili set productformulatype = 1 where dataownercode = 'ARR' and deprecated = 'Bubu';

-- GTFS: feed_info.txt
COPY (
SELECT
'OVapi' as feed_publisher_name,
'http://ovapi.nl/' as feed_publisher_url,
'nl' as feed_lang,
replace(cast(min(validdate) AS text), '-', '') as feed_start_date,
replace(cast(max(validdate) AS text), '-', '') as feed_end_date,
now() as feed_version 
FROM 
operday
) TO '/tmp/feed_info.txt' WITH CSV HEADER;

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
        FROM (SELECT DISTINCT ON (dataownercode,userstopcode) * FROM usrstop as u WHERE (getin = true or getout = true)) AS u,
              point AS p
        WHERE u.dataownercode = p.dataownercode AND
               u.userstopcode = p.pointcode AND
               u.version = p.version AND
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

-- For Veolia :/ Take everything as bus. Just dont do the FastFerry like this ;)
alter table line add column transporttype VARCHAR(5);
update line set transporttype = CASE WHEN (linepublicnumber = 'FF') THEN 'BOAT' ELSE 'BUS' END where transporttype is null;

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
CASE WHEN (line.dataownercode = 'ARR' and line.lineplanningnumber like '150__') THEN 'WATERBUS'
     WHEN (line.dataownercode = 'HTM' and line.lineplanningnumber in (select distinct lineplanningnumber from jopatili where confinrelcode = 'SGHB')) THEN 'HTMBUZZ'
     ELSE line.dataownercode END AS agency_id,
linepublicnumber AS route_short_name,
CASE WHEN (linepublicnumber <> linename) THEN linename ELSE NULL END AS route_long_name,
route_type AS route_type,
localized_route_type
FROM gtfs_route_type,line,
-- Could cause some wrong productformula as we're looking at the whole line and productformula can differ per journeypttern
(select distinct version,dataownercode,lineplanningnumber,productformulatype from jopatili) as pf LEFT JOIN productformula USING (productformulatype)
WHERE 
coalesce(line.transporttype,'BUS') = gtfs_route_type.transporttype AND 
line.transporttype != 'TRAIN' AND
pf.version = line.version AND
pf.dataownercode = line.dataownercode AND
pf.lineplanningnumber = line.lineplanningnumber
ORDER BY line.dataownercode,line.lineplanningnumber,line.version DESC
) TO '/tmp/routes.txt' WITH CSV HEADER;


-- GTFS: calendar_dates (Schedules en passeertijden)
COPY (
SELECT
version||'|'||dataownercode||'|'||organizationalunitcode||'|'||schedulecode||'|'||scheduletypecode AS service_id,
replace(CAST(validdate AS TEXT), '-', '') AS "date",
1 AS exception_type
FROM
operday
) TO '/tmp/calendar_dates.txt' WITH CSV HEADER;

alter table pujopass add column wheelchairaccessible VARCHAR(13);

create table gtfs_route_bikes_allowed (
        "dataownercode"      VARCHAR(10)   NOT NULL,
        "lineplanningnumber" VARCHAR(10)   NOT NULL,
        "trip_bikes_allowed" int4,
        PRIMARY KEY ("dataownercode", "lineplanningnumber")
);
insert into gtfs_route_bikes_allowed values ('GVB','900',2);
insert into gtfs_route_bikes_allowed values ('GVB','901',2);
insert into gtfs_route_bikes_allowed values ('GVB','902',2);
insert into gtfs_route_bikes_allowed values ('GVB','904',2);
insert into gtfs_route_bikes_allowed values ('GVB','905',2);
insert into gtfs_route_bikes_allowed values ('GVB','906',2);
insert into gtfs_route_bikes_allowed values ('GVB','907',2);
insert into gtfs_route_bikes_allowed values ('GVB','26',2);
insert into gtfs_route_bikes_allowed values ('GVB','50',2);
insert into gtfs_route_bikes_allowed values ('GVB','51',2);
insert into gtfs_route_bikes_allowed values ('GVB','52',2);
insert into gtfs_route_bikes_allowed values ('GVB','53',2);
insert into gtfs_route_bikes_allowed values ('GVB','54',2);
insert into gtfs_route_bikes_allowed values ('CXX','N419',2);
insert into gtfs_route_bikes_allowed values ('CXX','Z020',2);
insert into gtfs_route_bikes_allowed values ('CXX','Z050',2);
insert into gtfs_route_bikes_allowed values ('CXX','Z060',2);
insert into gtfs_route_bikes_allowed values ('VTN','26',2);

insert into gtfs_route_bikes_allowed values ('ARR','17090',2);
insert into gtfs_route_bikes_allowed values ('ARR','17194',2);
insert into gtfs_route_bikes_allowed values ('ARR','17196',2);
insert into gtfs_route_bikes_allowed values ('ARR','15020',2);
insert into gtfs_route_bikes_allowed values ('ARR','15021',2);
insert into gtfs_route_bikes_allowed values ('ARR','15022',2);
insert into gtfs_route_bikes_allowed values ('ARR','15023',2);
insert into gtfs_route_bikes_allowed values ('ARR','15024',2);


-- GTFS: trips.txt (Schedules en passeertijden)
--
-- Missing:
--   KV1 doesn't disclose information about block_id (same busses used for the next trip)
-- 
-- Cornercases:
--   StopOrder and TimingLinkOrder expect a stable minimum.
COPY (
SELECT DISTINCT ON (trip_id)
p.dataownercode||'|'||p.lineplanningnumber AS route_id,
p.version||'|'||p.dataownercode||'|'||p.organizationalunitcode||'|'||p.schedulecode||'|'||p.scheduletypecode AS service_id,
p.version||'|'||p.dataownercode||'|'||p.organizationalunitcode||'|'||p.schedulecode||'|'||p.scheduletypecode||'|'||p.lineplanningnumber||'|'||p.journeynumber 
AS trip_id,
p.journeynumber as trip_short_name,
d.destnamefull AS trip_headsign,
(cast(j.direction AS int4) - 1) AS direction_id,
jt.version||'|'||jt.dataownercode||'|'||jt.lineplanningnumber||'|'||jt.journeypatterncode AS shape_id,
wheelchair_accessible,
trip_bikes_allowed
FROM jopa AS j, jopatili AS jt, dest AS d, gtfs_wheelchair_accessibility as g,
pujopass as p LEFT JOIN gtfs_route_bikes_allowed using (dataownercode,lineplanningnumber)
WHERE
coalesce(p.wheelchairaccessible,'UNKNOWN') = g.wheelchairaccessible AND
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
stoporder = 1 AND
j.dataownercode||'|'||j.lineplanningnumber not in (select dataownercode||'|'||lineplanningnumber from line where transporttype = 'TRAIN')  
ORDER BY trip_id,timinglinkorder ASC
) TO '/tmp/trips.txt' WITH CSV HEADER;

-- workaround for some KV1
alter table jopatili add column productformulatype DECIMAL(4);

COPY (
SELECT
p.version||'|'||p.dataownercode||'|'||p.organizationalunitcode||'|'||p.schedulecode||'|'||p.scheduletypecode||'|'||p.lineplanningnumber||'|'||p.journeynumber 
AS trip_id,
coalesce(p.targetarrivaltime,p.targetdeparturetime) AS arrival_time,
coalesce(p.targetdeparturetime,p.targetarrivaltime) AS departure_time,
p.dataownercode||'|'||p.userstopcode AS stop_id,
stop_headsign,
p.stoporder AS stop_sequence,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getin as integer) END as pickup_type,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getout as integer) END as drop_off_type
FROM usrstop as u,
pujopass as p
  LEFT JOIN (SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode) 
   version,dataownercode,lineplanningnumber,journeypatterncode,productformulatype FROM jopatili WHERE productformulatype != 18
   ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder ASC) as prodform USING 
   (version,dataownercode,lineplanningnumber,journeypatterncode)
  LEFT JOIN (
	SELECT j.version,j.dataownercode,j.lineplanningnumber,j.journeypatterncode,j.stoporder,destnamefull as stop_headsign
	FROM
	dest as d,
	(   SELECT
	        version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder as stoporder,userstopcodebegin as userstopcode,destcode,istimingstop,productformulatype FROM JOPATILI
            UNION
            (SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
	        version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder+1 as stoporder,userstopcodeend as userstopcode,destcode,istimingstop,productformulatype FROM JOPATILI
 	    ORDER BY version ASC, dataownercode ASC,lineplanningnumber ASC,journeypatterncode ASC,timinglinkorder DESC)) as j,
	(    SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
 	     version,dataownercode,lineplanningnumber,journeypatterncode,destcode FROM JOPATILI
  	   ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder ASC) as t
	WHERE
	t.version = j.version AND
	t.dataownercode = j.dataownercode AND
	t.lineplanningnumber = j.lineplanningnumber AND
	t.journeypatterncode = j.journeypatterncode AND
	t.destcode <> j.destcode AND
	j.version = d.version AND
	j.destcode = d.destcode) as jt USING (version,dataownercode,lineplanningnumber,journeypatterncode,stoporder)
WHERE
p.version = u.version AND
p.userstopcode = u.userstopcode AND
(u.getin or u.getout) AND
p.dataownercode||'|'||p.lineplanningnumber not in (select dataownercode||'|'||lineplanningnumber from line where transporttype = 'TRAIN')
)TO '/tmp/stop_times.txt' WITH CSV HEADER;
