import zipfile
import psycopg2
import sys
import os
import optparse
from lxml import etree
import urllib2

importorder = ['DEST','LINE','CONAREA','CONFINREL','USRSTAR','USRSTOP','POINT','TILI','LINK','POOL','JOPA','JOPATILI','ORUN','ORUNORUN','SPECDAY','PEGR','EXCOPDAY','PEGRVAL','TIVE','TIMDEMGRP','TIMDEMRNT','PUJO','SCHEDVERS','PUJOPASS','OPERDAY']

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
    lines = schedule.split('\r\n')
    if lines[0].split('|')[1] == 'VersionNumber':
        firstline = 1
    else:
        firstline = 0
    validfrom = '3000-01-01'
    validthru = '1900-01-01'
    for line in lines[firstline:-1]:
       values = line.split('|')
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
    header = (zipfile.read(files['DEST']).split('\r\n')[0].split('|')[1] == 'VersionNumber')
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
    cur.execute("INSERT INTO version (dataownercode,validfrom,validthru,filename,dataownerversion) VALUES (%(DataOwnerCode)s,%(ValidFrom)s,%(ValidThru)s,%(Key)s,%(DataOwnerVersion)s)",meta)
    for x in reversed(importorder):
        cur.execute("update %s_delta set version = (select last_value from version_version_seq)" % (x))
    conn.commit()

def mergedelta(dataownercode,conn):
    print 'merging delta into baseline'
    cur = conn.cursor()
    if dataownercode not in ['HTM']: #HTM doesn't publish KV1 with overlap    
        cur.execute("""
DELETE FROM operday as o 
WHERE EXISTS
(    SELECT 1 FROM operday_delta as d WHERE o.organizationalunitcode = d.organizationalunitcode 
     AND o.dataownercode = d.dataownercode and o.validdate = d.validdate)
""")
    for x in importorder:
        cur.execute("INSERT INTO %s (select * from %s_delta)" % (x,x))
    print 'Delta merged'
    cur.close()
    conn.commit() 

def purge(conn):
    print 'delete expired deltas'
    cur = conn.cursor()
    cur.execute("UPDATE version SET validthru = (select max(validdate) from operday where version = version.version group by version);")
    conn.commit()
    cur.execute("INSERT INTO purgedversion (SELECT * FROM version WHERE validthru < date 'yesterday' or validthru is null)")
    cur.execute("DELETE FROM version WHERE validthru < date 'yesterday' or validthru is null")
    cur.close()
    conn.commit()

def fileimported(conn,key,dataownerversion):
    cur = conn.cursor()
    cur.execute("SELECT (EXISTS (SELECT 1 FROM version WHERE filename = %s AND dataownerversion = %s) or EXISTS (SELECT 1 FROM purgedversion WHERE filename = %s AND dataownerversion = %s))",[key,dataownerversion,key,dataownerversion])
    try:
        return cur.fetchone()[0]
    finally:
        cur.close()

def importfile(conn,path,filename,dataownerversion,key):
    if path is None or path == '':
        path = '.'  
    cleandelta(conn)
    if fileimported(conn,filename,dataownerversion):
        print 'Same version of file %s already imported' % (filename)
        return
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
    setversion(conn,meta)
    mergedelta(meta['DataOwnerCode'],conn)
    cleandelta(conn)
    purge(conn)

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

def sync(conn,kv1index):
    tree = etree.parse(kv1index)
    index = []
    for periode in tree.findall('periode'):
        file = {}
        file['key'] = periode.attrib['key']
        file['filename'] = periode.find('zipfile').text
        file['dataownerversion'] = periode.find('versie').text
        file['ispublished'] = periode.find('isgepubliceerd').text
        file['validfrom'] = periode.find('startdatum').text
        file['validthru'] = periode.find('einddatum').text
        index.append(file)
    index = multikeysort(index, ['validfrom', '-validthru'])
    for f in index:
        print 'key: '+f['key']+' filename: ' + f['filename'] + ' startdatum ' + f['validfrom'][0:10] + ' einddatum ' + f['validthru'][0:10] 
        if not fileimported(conn,f['key'],f['dataownerversion']) and f['ispublished'] == 'true':
            print 'Import file %s version %s' % (f['filename'],str(f['dataownerversion']))
            url = '/'.join(kv1index.strip().split('/')[:-1])+'/'+f['filename']
            downloadfile(f['filename'],url)
            importfile(conn,'/tmp',f['filename'],f['dataownerversion'],f['key'])

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
    opts, args = parser.parse_args()
    if not opts.database:
        print "Name of the database is mandatory"
        parser.print_help()
        exit(-1)
    if opts.purge:
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        purge(conn)
        conn.close()
    if opts.addfile:
        path, filename = os.path.split(opts.addfile)
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        importfile(conn,path,filename,1,None)
        purge(conn)
        conn.close()
    elif opts.addfolder:
        files = os.listdir(opts.addfolder)
        files.sort(key=lambda f: os.path.getmtime(os.path.join(opts.addfolder, f)))
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        for file in files:
            importfile(conn,opts.addfolder,file,1,None)
        purge(conn)
        conn.close()
    elif opts.kv1index:
        conn = psycopg2.connect("dbname='%s'" % (opts.database))
        sync(conn,opts.kv1index);
        conn.close()
if __name__ == '__main__':
    main()


