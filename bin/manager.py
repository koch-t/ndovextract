import zipfile
import psycopg2
import sys
from datetime import datetime,timedelta
import os
import optparse
from lxml import etree
import urllib2
import logging
from kv1compress import generatetimedemandgroups

importorder = ['DEST','LINE','CONAREA','CONFINREL','POINT','USRSTAR','USRSTOP','TILI','LINK','POOL','JOPA','JOPATILI','ORUN','ORUNORUN','SPECDAY','PEGR','EXCOPDAY','PEGRVAL','TIVE','TIMDEMGRP','TIMDEMRNT','PUJO','SCHEDVERS','PUJOPASS','SCHEDPUJO','OPERDAY']
versionheaders = ['Version_Number','VersionNumber','VERSIONNUMBER','version']
log = logging.getLogger('ndovextract')

def table(filename):
    filename = filename.split('.TMI')[0]
    return filename.rstrip('X').rstrip('.csv')

def filelist(zipfile):
    files = {}
    filenames = zipfile.namelist()
    for file in filenames:
        files[table(file.split('/')[-1])] = file
    return files

def encodingof(dataownercode):
    if dataownercode in ['QBUZZ','CXX','EBS']:
        return 'ISO-8859-15'
    else:
        return 'UTF-8'

def cleandelta(conn):
    cur = conn.cursor()
    print 'Clear delta tables'
    for x in reversed(importorder):
        cur.execute("delete from %s_delta" % (x))
        conn.commit()

def metadata(schedule):
    lines = schedule.split('\n')
    if lines[0].split('|')[1] in versionheaders:
        firstline = 1
    else:
        firstline = 0
    validfrom = '3000-01-01'
    validthru = '1900-01-01'
    for line in lines[firstline:]:
       values = line.split('|')
       if len(values) < 8:
           continue
       dataowner = values[3]
       if values[7] < validfrom:
           validfrom = values[7]
       if values[7] > validthru:
           validthru = values[7]
    return {'ValidFrom' : validfrom, 'ValidThru' : validthru, 'DataOwnerCode' : dataowner}

def importzip(conn,filename,zipfile):
    print 'Import ' + filename
    files = filelist(zipfile)
    cur = conn.cursor()
    meta = metadata(zipfile.read(files['OPERDAY']))
    if datetime.strptime(meta['ValidThru'].replace('-',''),'%Y%m%d') < (datetime.now() - timedelta(days=1)):
        return meta
    header = (zipfile.read(files['DEST']).split('\n')[0].split('|')[1] in versionheaders)
    encoding = encodingof(meta['DataOwnerCode'])
    for table in importorder:
        if table in files:
            f = zipfile.open(files[table])
            table = table+'_delta'
            if header:
                cur.copy_expert("COPY %s FROM STDIN WITH DELIMITER AS '|' NULL AS '' CSV HEADER ENCODING '%s'" % (table,encoding),f)
            else:
                cur.copy_expert("COPY %s FROM STDIN WITH DELIMITER AS '|' NULL AS '' CSV ENCODING '%s'" % (table,encoding),f)
    conn.commit()
    cur.close()
    return meta

def setversion(conn,meta):
    cur = conn.cursor()
    if 'DataOwnerVersion' not in meta:
        meta['DataOwnerVersion'] = 1
    conn.commit()
    if meta['DataOwnerCode'] == 'HTM':
        if 'buzz' in meta['Key'].lower():
            meta['UnitCode'] = 'SGHB'
        else:
            meta['UnitCode'] = 'SGHR'
        #meta['UnitCode'] = cur.fetchone()[0]
    else:
        meta['UnitCode'] = meta['DataOwnerCode']
    cur.execute("INSERT INTO version (dataownercode,validfrom,validthru,filename,dataownerversion,unitcode) VALUES (%(DataOwnerCode)s,%(ValidFrom)s,%(ValidThru)s,%(Key)s,%(DataOwnerVersion)s,%(UnitCode)s)",meta)
    for x in reversed(importorder):
        cur.execute("update %s_delta set version = (select last_value from version_version_seq)" % (x))
    conn.commit()

def mergedelta(dataownercode,conn,delta):
    if delta:
        print 'Merging delta in to the baseline'
    else:
        print 'Merging KV1'
    cur = conn.cursor()
    if dataownercode in ['SYNTUS']:
        cur.execute("""
DELETE FROM OPERDAY_delta as o WHERE validdate < (SELECT validfrom FROM schedvers_delta as s WHERE s.schedulecode = o.schedulecode AND 
s.scheduletypecode = o.scheduletypecode AND s.version = o.version AND s.organizationalunitcode = o.organizationalunitcode)""")
        conn.commit()
    if dataownercode in ['HTM']:
        cur.execute("""
DELETE FROM operday as o
WHERE EXISTS
( SELECT 1 FROM schedvers_delta as d LEFT JOIN version as dv USING(version), version as ov
WHERE 
ov.version = o.version AND
ov.unitcode = dv.unitcode AND
o.organizationalunitcode = d.organizationalunitcode AND
o.dataownercode = d.dataownercode AND 
o.validdate between d.validfrom and d.validthru)
""")
        conn.commit()
    elif delta and dataownercode in ['GVB']: #Cut away the data within the boundaries of the schervers validfrom/validthru
        cur.execute("""
DELETE FROM operday as o
WHERE EXISTS
( SELECT 1 FROM schedvers_delta as d WHERE o.organizationalunitcode = d.organizationalunitcode
AND o.dataownercode = d.dataownercode and o.validdate between validfrom and validthru)""")
    elif delta and dataownercode not in ['HTM','QBUZZ']: #HTM doesn't publish KV1 with overlap    
        cur.execute("""
DELETE FROM operday as o
WHERE EXISTS
( SELECT 1 FROM operday_delta as d WHERE o.organizationalunitcode = d.organizationalunitcode
AND o.dataownercode = d.dataownercode and o.validdate = d.validdate)
""")
    elif not delta and dataownercode not in ['QBUZZ']:
        cur.execute("""
DELETE FROM operday as o
WHERE validdate >=
( SELECT min(validdate) FROM operday_delta as d WHERE o.organizationalunitcode = d.organizationalunitcode AND o.dataownercode = d.dataownercode)
""")
    for x in importorder:
        cur.execute("INSERT INTO %s (select * from %s_delta)" % (x,x))
    print 'Delta merged'
    cur.close()
    conn.commit() 

def purge(conn,delta=False):
    print 'delete expired deltas'
    cur = conn.cursor()
    cur.execute("UPDATE version SET validthru = (select max(validdate) from operday where version = version.version group by version);")
    conn.commit()
    cur.execute("select true in (select (validthru < date 'yesterday' or validthru is null) FROM version);")
    changed = cur.fetchone()[0]
    cur.execute("INSERT INTO purgedversion (SELECT * FROM version WHERE validthru < date 'yesterday' or validthru is null)")
    cur.execute("DELETE FROM version WHERE validthru < date 'yesterday' or validthru is null")
    cur.close()
    conn.commit()
    return changed

def fileimported(conn,key,dataownerversion):
    cur = conn.cursor()
    cur.execute("SELECT (EXISTS (SELECT 1 FROM version WHERE filename = %s AND dataownerversion = %s) or EXISTS (SELECT 1 FROM purgedversion WHERE filename = %s AND dataownerversion = %s))",[key,dataownerversion,key,dataownerversion])
    try:
        return cur.fetchone()[0]
    finally:
        cur.close()

def importfile(conn,path,filename,dataownerversion,key,delta,compress=False):
    if path is None or path == '':
        path = '.'  
    cleandelta(conn)
    if fileimported(conn,filename,dataownerversion):
        print 'Same version of file %s already imported' % (filename)
        return False
    print filename
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    if 'Csv.zip' in zip.namelist():
        zipfile.ZipFile.extract(zip,'Csv.zip','/tmp')
        zip = zipfile.ZipFile('/tmp/Csv.zip','r')
    meta = importzip(conn,filename,zip)
    if key is None:
        meta['Key'] = filename
    else: 
        meta['Key'] = key
    meta['DataOwnerVersion'] = dataownerversion
    if compress:
        generatetimedemandgroups(conn,delta=True)
    setversion(conn,meta)
    mergedelta(meta['DataOwnerCode'],conn,delta)
    cleandelta(conn)
    purge(conn,delta=delta)
    return True

def downloadfile(filename,url):
    u = urllib2.urlopen(url)
    f = open('/tmp/'+filename, 'wb')

    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (filename, file_size)

    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break
        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print status,
    print
    f.close()

def multikeysort(items, columns):
    from operator import itemgetter
    comparers = [ ((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]  
    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, cmp=comparer)

def deletedelta(conn,key):
    print 'delete delta with key ' + str(key)
    cur = conn.cursor()
    cur.execute("DELETE FROM version WHERE filename = %s",[key])
    cur.close()
    conn.commit()

def sync(conn,kv1index,compress=False):
    tree = etree.parse(kv1index)
    index = []
    for periode in tree.findall('periode'):
        file = {}
        file['key'] = periode.attrib['key']
        file['filename'] = periode.find('zipfile').text
        file['dataownerversion'] = periode.find('versie').text
        file['ispublished'] = periode.find('isgepubliceerd').text
        file['publishdate'] = periode.find('publicatiedatum').text
        print periode.find('isbaseline').text
        file['isbaseline'] = (periode.find('isbaseline').text == 'true')
        if file['ispublished'] == 'false':
            deletedelta(conn,file['key'])
        file['validfrom'] = periode.find('startdatum').text
        file['validthru'] = periode.find('einddatum').text
        file['index'] = int(periode.find('index').text)
        if file['key'] == 'a00bac99-e404-4783-b2f7-a39d48747999':
            file['isbaseline'] = True
        index.append(file)
    index = multikeysort(index, ['-isbaseline','index','publishdate'])
    changed = False
    for f in index:
        print 'key: '+f['key']+' filename: ' + f['filename'] + ' isbaseline: ' + str(f['isbaseline']) + ' startdatum ' + f['validfrom'][0:10] + 'einddatum ' + f['validthru'][0:10] 
        if not fileimported(conn,f['key'],f['dataownerversion']) and f['ispublished'] == 'true':
            print 'Import file %s version %s' % (f['filename'],str(f['dataownerversion']))
            url = '/'.join(kv1index.strip().split('/')[:-1])+'/'+f['filename']
            downloadfile(f['filename'],url)
            changed = True
            importfile(conn,'/tmp',f['filename'],f['dataownerversion'],f['key'],True,compress)
    return changed

def main():
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-d", "--database", dest="database",
                    action="store",
                    default=False,
                    help="Name of the database to be used")
    parser.add_option("-p", "--purge", dest="purge",
                    action="store_true",
                    default=False,
                    help="Purge expired definitions")
    parser.add_option("-a", "--add", dest="addfile",
                    action="store",
                    default=False,
                    help="Add kv1 zipfile")
    parser.add_option("-f", "--folder", dest="addfolder",
                    action="store",
                    default=False,
                    help="Add folder with Koppelvlak1 zipfiles")
    parser.add_option("-s", "--sync", dest="kv1index",
                    action="store",
                    default=False,
                    help="Sync with KV1index feed online")
    parser.add_option("-c", "--compress", dest="compress",
                    action="store_true",
                    default=False,
                    help="Generate timedemandgroupcodes")
    parser.add_option("-n", "--sortbyname", dest="sortbyname",
                    action="store_true",
                    default=False,
                    help="Order by filename instead of date modified, use this only with KV1 with a regular filename which includes a order")
    parser.add_option("-x", "--delta", dest="delta",
                    action="store_true",
                    default=False,
                    help="With this mode the new KV1 is fit in the old KV1 instead of deleting everything from operday where validdate >= validfromnewkv1 ")
    opts, args = parser.parse_args()
    changed = False
    if not opts.database:
        print "Name of the database is mandatory"
        parser.print_help()
        exit(-1)
    if opts.purge:
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        purge(conn,delta=opts.delta)
        conn.close()
    if opts.addfile:
        path, filename = os.path.split(opts.addfile)
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        changed = importfile(conn,path,filename,1,None,opts.delta,opts.compress)
        changed = changed or purge(conn,delta=opts.delta)
        conn.close()
    elif opts.addfolder:
        files = os.listdir(opts.addfolder)
        if opts.sortbyname:
            files = sorted(files)
        else:
            files.sort(key=lambda f: os.path.getmtime(os.path.join(opts.addfolder, f)))
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        for file in files:
          try:
            if file[-4:].lower() == '.zip' and importfile(conn,opts.addfolder,file,1,None,opts.delta,opts.compress):
                changed = True
          except Exception as e:
            conn.rollback()
            raise
        if purge(conn,delta=opts.delta):
            changed = True
        conn.close()
    elif opts.kv1index:
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        changed = sync(conn,opts.kv1index,opts.compress);
        changed = changed or purge(conn,delta=True)
        conn.close()
    if not changed:
        sys.exit(1)
if __name__ == '__main__':
    main()
