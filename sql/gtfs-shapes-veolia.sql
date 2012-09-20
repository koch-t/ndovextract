COPY (
SELECT
'VTN' as agency_id,
'Veolia' as agency_name,
'http://www.veolia.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
) TO '/tmp/agency.txt' WITH CSV HEADER;

-- GTFS: shapes.txt
-- -- Missing:
--  KV1 support for LinkValidFrom
--  GTFS support for shape_dist_traveled (summation of distancesincestartoflink)
--  ** disabled transporttype **
COPY (
SELECT DISTINCT shape_id,
      CAST(ST_Y(the_geom) AS NUMERIC(8,5)) AS shape_pt_lat,
      CAST(ST_X(the_geom) AS NUMERIC(7,5)) AS shape_pt_lon,
      shape_pt_sequence
FROM
 (SELECT jopatili.version||'|'||jopatili.dataownercode||'|'||jopatili.lineplanningnumber||'|'||jopatili.journeypatterncode AS shape_id,
  ST_Transform(st_setsrid(st_makepoint(locationx_ew, locationy_ns), 28992), 4326) AS the_geom,
  row_number() over (PARTITION BY jopatili.version,jopatili.dataownercode, jopatili.lineplanningnumber, jopatili.journeypatterncode ORDER BY 
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
    --AND pool.transporttype = line.transporttype
  ORDER BY
           jopatili.version,
           jopatili.dataownercode,
           jopatili.lineplanningnumber,
           jopatili.journeypatterncode,
           jopatili.timinglinkorder,
           pool.distancesincestartoflink) AS KV1
) TO '/tmp/shapes.txt' WITH CSV HEADER;

-- GTFS: calendar (Schedules en passeertijden)
COPY (
SELECT
version||'|'||dataownercode||'|'||organizationalunitcode||'|'||schedulecode||'|'||scheduletypecode AS service_id,
cast((description like '%Weekday%' or description like '%Mon-Wed%' or description like '%Monday%') AS int4) AS monday,
cast((description like '%Weekday%' or description like '%Mon-Wed%' or description like '%Tuesday%') AS int4) AS tuesday,
cast((description like '%Weekday%' or description like '%Mon-Wed%' or description like '%Wednesday%') AS int4) AS wednesday,
cast((description like '%Weekday%' or description like '%Thu-Fri%' or description like '%Thursday%') AS int4) AS thursday,
cast((description like '%Weekday%' or description like '%Thu-Fri%' or description like '%Friday%') AS int4) AS friday,
cast((description like '%Saturday%') AS int4) AS saturday,
cast((description like '%Sunday%') AS int4) AS sunday,
replace(CAST(validfrom AS TEXT), '-', '') AS start_date,
replace(CAST(validthru AS TEXT), '-', '') AS end_date
FROM
schedvers
) TO '/tmp/calendar.txt' WITH CSV HEADER;
