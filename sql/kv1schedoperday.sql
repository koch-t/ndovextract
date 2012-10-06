INSERT INTO SCHEDVERS(
SELECT DISTINCT on (pu.version,pu.dataownercode,pu.timetableversioncode,pu.organizationalunitcode,pu.periodgroupcode,pu.specificdaycode,pu.daytype)
'SCHEDVERS' as tablename,
pu.version,
pu.implicit,
pu.dataownercode,
pu.organizationalunitcode,
pu.periodgroupcode as schedulecode,
pu.daytype as scheduletypecode,
t.validfrom,
coalesce(t.validthru,p.validthru)  as validthru,
pu.version||'-'||pu.dataownercode||'-'||pu.timetableversioncode||'-'||pu.organizationalunitcode||'-'||pu.periodgroupcode||'-'||pu.specificdaycode||'-'||pu.daytype 
as description
FROM
tive as t, pegrval as p,pujo as pu
WHERE
t.dataownercode = p.dataownercode AND
t.version = p.version AND
t.organizationalunitcode = p.organizationalunitcode AND
t.periodgroupcode = p.periodgroupcode AND
t.validfrom != p.validfrom AND
t.version = pu.version AND
t.dataownercode = pu.dataownercode AND
t.organizationalunitcode = pu.organizationalunitcode AND
t.periodgroupcode = pu.periodgroupcode AND
t.specificdaycode = pu.specificdaycode
);

INSERT INTO OPERDAY(
SELECT
'OPERDAY' as tablename,
version,
implicit,
dataownercode,
organizationalunitcode,
schedulecode,
scheduletypecode,
validdate,
null as description
FROM 
( SELECT
  version,
  implicit,
  dataownercode,
  organizationalunitcode,
  schedulecode,
  scheduletypecode,
  CAST (generate_series(validfrom,validthru,'1 day') as date) as validdate
  FROM schedvers) as x
WHERE
position( cast ((extract(dow from validdate) +1) as text) in scheduletypecode) != 0
);

--TODO handle excopdays correctly
--select * from operday where validdate in (select cast (validdate as date) from excopday);

INSERT INTO PUJOPASS(
SELECT
'PUJOPASS' as tablename,
p.version as version,
p.implicit,
p.dataownercode,
p.organizationalunitcode,
p.periodgroupcode as schedulecode,
p.daytype as scheduletypecode,
p.lineplanningnumber,
p.journeynumber,
1 as stoporder,
p.journeypatterncode,
userstopcodebegin as userstopcode,
departuretime as targetarrivaltime,
departuretime as targetdeparturetime,
wheelchairaccessible,
dataownerisoperator
FROM
pujo as p, 
( SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
  version,dataownercode,lineplanningnumber,journeypatterncode,userstopcodebegin
  FROM jopatili
  ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder
 ) as j
WHERE 
p.version = j.version AND
p.dataownercode = j.dataownercode AND
p.lineplanningnumber = j.lineplanningnumber AND
p.journeypatterncode = j.journeypatterncode
UNION
SELECT
'PUJOPASS' as tablename,
p.version as version,
p.implicit,
p.dataownercode,
p.organizationalunitcode,
p.periodgroupcode as schedulecode,
p.daytype as scheduletypecode,
p.lineplanningnumber,
p.journeynumber,
(timinglinkorder + 2) as stoporder,
p.journeypatterncode,
userstopcodebegin as userstopcode,
add32time(departuretime,totaldrivetime) as targetarrivaltime,
add32time(departuretime,cast((totaldrivetime+stopwaittime)as integer)) as targetdeparturetime,
wheelchairaccessible,
dataownerisoperator
FROM
pujo as p, timdempass as t
WHERE
p.version = t.version AND
p.dataownercode = t.dataownercode AND
p.lineplanningnumber = t.lineplanningnumber AND
p.journeypatterncode = t.journeypatterncode AND
p.timedemandgroupcode = t.timedemandgroupcode
);
