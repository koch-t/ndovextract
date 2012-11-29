import psycopg2
import cStringIO
import sys

"""
CREATE TABLE "timdempass" (
    "tablename"         VARCHAR(10)   NOT NULL,
    "version"             INTEGER    NOT NULL,
    "implicit"            CHAR(1)       NOT NULL,
    "dataownercode"       VARCHAR(10)   NOT NULL,
    "lineplanningnumber"  VARCHAR(10)   NOT NULL,
    "journeypatterncode"  VARCHAR(10)   NOT NULL,
    "timedemandgroupcode" VARCHAR(10)   NOT NULL,
    "timinglinkorder"     DECIMAL(3)    NOT NULL,
    "userstopcodebegin"   VARCHAR(10)   NOT NULL,
    "userstopcodeend"     VARCHAR(10)   NOT NULL,
    "totaldrivetime"      INTEGER       NOT NULL,
    "stopwaittime"        DECIMAL(5)    NOT NULL,
    PRIMARY KEY ("version","dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode", "timinglinkorder"),
    FOREIGN KEY ("version","dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode") REFERENCES "timdemgrp" 
("version","dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode"),
    FOREIGN KEY ("version","dataownercode", "lineplanningnumber", "journeypatterncode", "timinglinkorder") REFERENCES "jopatili" 
("version","dataownercode", "lineplanningnumber", "journeypatterncode", "timinglinkorder")
);
"""

try:
    conn = psycopg2.connect("dbname='%s'" % (sys.argv[1]))
except:
    print "I am unable to connect to the database"
    raise

cur = conn.cursor()
cur.execute("""DELETE FROM timdempass;""")
cur.execute("""
SELECT 
timdemrnt.version, 
implicit, 
dataownercode,
lineplanningnumber,
journeypatterncode,
timedemandgroupcode,
timinglinkorder,
timdemrnt.userstopcodebegin,
timdemrnt.userstopcodeend,
totaldrivetime,
stopwaittime,
distance 
FROM timdemrnt,
(select version,userstopcodebegin, userstopcodeend, min(distance) as distance from link group by version,userstopcodebegin, userstopcodeend) as link
WHERE
timdemrnt.userstopcodebegin = link.userstopcodebegin AND
timdemrnt.userstopcodeend = link.userstopcodeend AND
link.version = timdemrnt.version
ORDER BY 
timdemrnt.version,dataownercode, lineplanningnumber, journeypatterncode, timedemandgroupcode, timinglinkorder
""")
rows = cur.fetchall()

prev_pk = None
totaldrivetime = 0

f = cStringIO.StringIO()

buf = []

for i in range(0, len(rows)):
    pk = '|'.join([str(x) for x in rows[i][0:6]])
    if len(buf) > 0 and (pk != prev_pk or rows[i][9] != 0):
        if (pk == prev_pk):
            buf.append(i)

        totaldistance = sum([int(rows[x][11]) for x in buf])
        totaltime     = sum([int(rows[x][9])  for x in buf])
        allocated     = 0
        for x in buf:
            rows[x] = list(rows[x])
        #    print 'old',rows[x][11],rows[x][9]
            allocate = int(totaltime * rows[x][11] / totaldistance)
            if allocate == 0:
                allocate = 4 # if it is an extremely short distance...
            allocated += allocate
            rows[x][9] = allocate
        #    print 'new',rows[x][11],rows[x][9]

        rows[buf[-1]][9] += (totaltime - allocated)
        # print 'new',rows[x][11],rows[x][9]
        # print '-'
        buf = []

    if pk != prev_pk:
        prev_pk = pk
        buf = []

    if rows[i][9] == 0 or (i < (len(rows) - 1) and rows[i+1][9] == 0):
        buf.append(i)

for row in rows:
    pk = '|'.join([str(x) for x in row[0:6]])
    if pk != prev_pk:
        totaldrivetime = int(row[9])
        prev_pk = pk
    else:
        totaldrivetime += int(row[9]) # for debug switch this line
        # totaldrivetime = int(row[9])

    f.write('\t'.join(['TIMDEMPASS'] + list([str(x) for x in row[0:9]]) + [str(totaldrivetime)] + [str(row[10])]) + '\n')
    totaldrivetime += int(row[10])

f.seek(0)

cur.copy_from(f, 'timdempass', columns=('tablename', 'version', 'implicit', 'dataownercode', 'lineplanningnumber', 'journeypatterncode', 'timedemandgroupcode', 'timinglinkorder', 'userstopcodebegin', 'userstopcodeend', 'totaldrivetime', 'stopwaittime'))
conn.commit()
