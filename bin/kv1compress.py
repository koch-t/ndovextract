import psycopg2
import cStringIO

"""
CREATE TABLE SCHEDPUJO (
    "tablename" VARCHAR(10) NOT NULL,
    version integer NOT NULL,
    implicit CHAR(1) NOT NULL,
    dataownercode character varying(10) NOT NULL,
    organizationalunitcode character varying(10) NOT NULL,
    schedulecode character varying(10) NOT NULL,
    scheduletypecode character varying(10) NOT NULL,
    lineplanningnumber character varying(10) NOT NULL,
    journeynumber numeric(6,0) NOT NULL,
   "timedemandgroupcode" VARCHAR(10) NOT NULL,
    journeypatterncode character varying(10) NOT NULL,
    departuretime char(8),
    wheelchairaccessible VARCHAR(13),
    dataownerisoperator boolean NOT NULL,
    PRIMARY KEY (version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber),
    FOREIGN KEY (version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode) REFERENCES schedvers(version, dataownercode,
organizationalunitcode, schedulecode, scheduletypecode),
    FOREIGN KEY ("version", "dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode") REFERENCES "timdemgrp"
("version", "dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode"),
    FOREIGN KEY (version, dataownercode, lineplanningnumber, journeypatterncode) REFERENCES jopa(version, dataownercode, lineplanningnumber,
journeypatterncode),
    FOREIGN KEY ("version", "dataownercode") REFERENCES "version" ("version", "dataownercode") ON DELETE CASCADE
);
CREATE TABLE SCHEDPUJO_delta (
    "tablename" VARCHAR(10) NOT NULL,
    version integer NOT NULL,
    implicit CHAR(1) NOT NULL,
    dataownercode character varying(10) NOT NULL,
    organizationalunitcode character varying(10) NOT NULL,
    schedulecode character varying(10) NOT NULL,
    scheduletypecode character varying(10) NOT NULL,
    lineplanningnumber character varying(10) NOT NULL,
    journeynumber numeric(6,0) NOT NULL,
   "timedemandgroupcode" VARCHAR(10) NOT NULL,
    journeypatterncode character varying(10) NOT NULL,
    departuretime char(8),
    wheelchairaccessible VARCHAR(13),
    dataownerisoperator boolean NOT NULL,
    PRIMARY KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber),
    FOREIGN KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode) REFERENCES schedvers(dataownercode,organizationalunitcode, schedulecode, scheduletypecode),
    FOREIGN KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode") REFERENCES "timdemgrp"
("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode"),
    FOREIGN KEY (dataownercode, lineplanningnumber, journeypatterncode) REFERENCES jopa(dataownercode, lineplanningnumber,
journeypatterncode),
);
"""

timdemrnt = {}
pujo = []

def timediff(timelow,timehigh):
    hourslow,minlow,secslow = timelow.split(':')
    hourshigh,minhigh,secshigh =  timehigh.split(':')
    low = (int(hourslow) * 60 * 60) + (int(minlow) * 60) + int(secslow)
    high = (int(hourshigh) * 60 * 60) + (int(minhigh) * 60) + int(secshigh)
    return high - low

def equals(timdemgrpleft,timdemgrpright):
    if len(timdemgrpleft) != len(timdemgrpright):
        return False
    for i in range(len(timdemgrpleft)):
        for j in range(len(timdemgrpleft[i])):
            if j != 6 and timdemgrpleft[i][j] != timdemgrpright[i][j]:
                return False
    return True

def merge(timdem_pk,timdemgrp):
    for groupcode,value in timdemrnt[timdem_pk].items():
        if equals(value,timdemgrp):
            return groupcode
    timdemgroupcode = len(timdemrnt[timdem_pk])
    for x in timdemgrp:
        x[6] = timdemgroupcode
    timdem_pk = '|'.join(str(x) for x in timdemgrp[0][0:6])
    timdemrnt[timdem_pk][timdemgroupcode] = timdemgrp
    return timdemgroupcode


def generatetimedemandgroups(conn,delta=False):
    timdemrnt = {}
    pujo = []
    if delta:
        tbl_postfix = '_delta'
    else:
        tbl_postfix = ''
    cur = conn.cursor()
    cur.execute("""
SELECT
version,dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber, stoporder,
journeypatterncode,userstopcode,coalesce(targetarrivaltime,targetdeparturetime),coalesce(targetdeparturetime,targetarrivaltime),wheelchairaccessible,dataownerisoperator
FROM PUJOPASS%s
ORDER BY
version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber, stoporder""" % (tbl_postfix))
    rows = cur.fetchall()
    i = 0
    pujof = cStringIO.StringIO()
    rntf = cStringIO.StringIO()
    grpf = cStringIO.StringIO()
    while i < (len(rows) - 1):
        pujo_pk = '|'.join(str(x) for x in rows[i][:7])
        timdemgrp = []
        pujo = ['SCHEDPUJO']
        pujo.extend(rows[i][:7])
        pujo.insert(2,'I')
        pujo.extend([rows[i][8],rows[i][11],rows[i][12],rows[i][13]])
        while (i+1 < len(rows) and pujo_pk == '|'.join(str(x) for x in rows[i+1][:7])):
            start = rows[i]
            to = rows[i+1]
            totaldrivetime = timediff(start[11],to[11])
            drivetime = timediff(start[11],to[10])
            stopcodestart = start[9]
            stopcodeend = to[9]
            line = ['TIMDEMRNT',start[0],'I',start[1],start[5],start[8],0,start[7],stopcodestart,stopcodeend,totaldrivetime,drivetime,0,0,totaldrivetime-drivetime,0]
            timdemgrp.append(line)
            i += 1
        timdem_pk = '|'.join(str(x) for x in timdemgrp[0][:6])
        if timdem_pk not in timdemrnt:
            timdemrnt[timdem_pk] = { 0 : timdemgrp}
            timdemgroupcode = 0
        else:
            timdemgroupcode = merge(timdem_pk,timdemgrp)
        pujo.insert(9,timdemgroupcode)
        pujof.write('|'.join(str(x) for x in pujo)+'\r\n')
        i += 1
    for timdem_pk,timdem in timdemrnt.items():
        for timdemgrp_pk,timdemgrp in timdem.items():
            grp = ['TIMDEMGRP']
            grp.extend(timdemgrp[0][1:7])
            grpf.write('|'.join(str(x) for x in grp)+'\r\n')
            for line in timdemgrp:
                rntf.write('|'.join(str(x) for x in line)+'\r\n')

    for x in ['schedpujo%s'% (tbl_postfix),'timdemrnt%s'% (tbl_postfix),'timdemgrp%s'% (tbl_postfix)]:
        cur.execute('delete from %s' % (x))
        conn.commit()

    grpf.seek(0)
    cur.copy_from(grpf,'timdemgrp%s'% (tbl_postfix),sep='|')
    conn.commit()
    grpf.close()

    rntf.seek(0)
    cur.copy_from(rntf,'timdemrnt%s'% (tbl_postfix),sep='|')
    conn.commit()
    rntf.close()

    pujof.seek(0)
    cur.copy_from(pujof,'schedpujo%s'% (tbl_postfix),sep='|')
    conn.commit()
    pujof.close()

    cur.close()
    conn.close()

