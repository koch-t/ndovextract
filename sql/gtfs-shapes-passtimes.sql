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

-- For Veolia :/ Take everything as bus. Just dont do the FastFerry like this ;)
alter table line add column transporttype VARCHAR(5);
update line set transporttype = CASE WHEN (linepublicnumber = 'FF') THEN 'BOAT' ELSE 'BUS' END where transporttype is null;

COPY (
SELECT DISTINCT ON (dataownercode,lineplanningnumber)
dataownercode||'|'||lineplanningnumber AS route_id,
dataownercode AS agency_id,
linepublicnumber AS route_short_name,
linename AS route_long_name,
route_type AS route_type
FROM line, gtfs_route_type
WHERE coalesce(line.transporttype,'BUS') = gtfs_route_type.transporttype
ORDER BY dataownercode,lineplanningnumber,version DESC
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

-- GTFS: trips.txt (Schedules en passeertijden)
--
-- Missing:
--   KV1 doesn't disclose information about block_id (same busses used for the next trip)
-- 
-- Cornercases:
--   StopOrder and TimingLinkOrder expect a stable minimum.
COPY (
select
p.dataownercode||'|'||p.lineplanningnumber AS route_id,
p.version||'|'||p.dataownercode||'|'||p.organizationalunitcode||'|'||p.schedulecode||'|'||p.scheduletypecode AS service_id,
p.version||'|'||p.dataownercode||'|'||p.organizationalunitcode||'|'||p.schedulecode||'|'||p.scheduletypecode||'|'||p.lineplanningnumber||'|'||p.journeynumber 
AS trip_id,
d.destnamefull AS trip_headsign,
(cast(j.direction AS int4) - 1) AS direction_id,
jt.version||'|'||jt.dataownercode||'|'||jt.lineplanningnumber||'|'||jt.journeypatterncode AS shape_id,
wheelchair_accessible
FROM pujopass AS p, jopa AS j, jopatili AS jt, dest AS d, gtfs_wheelchair_accessibility as g,
(select distinct version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode from operday) as v
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
jt.timinglinkorder = 1 AND
p.stoporder = 1 AND
p.version = v.version AND
p.dataownercode = v.dataownercode AND
p.organizationalunitcode = v.organizationalunitcode AND
p.schedulecode = v.schedulecode AND
p.scheduletypecode = v.scheduletypecode
) TO '/tmp/trips.txt' WITH CSV HEADER;

COPY (
SELECT
p.version||'|'||p.dataownercode||'|'||p.organizationalunitcode||'|'||p.schedulecode||'|'||p.scheduletypecode||'|'||p.lineplanningnumber||'|'||p.journeynumber 
AS trip_id,
coalesce(p.targetarrivaltime,p.targetdeparturetime) AS arrival_time,
coalesce(p.targetdeparturetime,p.targetarrivaltime) AS departure_time,
p.dataownercode||'|'||p.userstopcode AS stop_id,
p.stoporder AS stop_sequence,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getin as integer) END as pickup_type,
CASE WHEN (productformulatype in (2,35,36)) THEN 3 ELSE cast(not getout as integer) END as drop_off_type
FROM pujopass AS p, usrstop as u,
(select distinct version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode from operday) as v
WHERE p.dataownercode = u.dataownercode
and p.version = u.version
AND p.userstopcode = u.userstopcode
AND (u.getin = TRUE OR u.getout = TRUE) AND
p.version = v.version AND
p.dataownercode = v.dataownercode AND
p.organizationalunitcode = v.organizationalunitcode AND
p.schedulecode = v.schedulecode AND
p.scheduletypecode = v.scheduletypecode
) TO '/tmp/stop_times.txt' WITH CSV HEADER;
