#!/usr/bin/env python2

#Mirror KV1 zipfiles using a KV1index file
import sys
import os
from lxml import etree
import urllib2
import optparse

def downloadfile(filename,url):
    u = urllib2.urlopen(url)
    f = open(filename, 'wb')

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

def parsekv1index(tree):
    index = {}
    for periode in tree.findall('periode'):
        file = {}
        file['key'] = periode.attrib['key']
        file['filename'] = periode.find('zipfile').text
        file['dataownerversion'] = int(periode.find('versie').text)
        file['ispublished'] = periode.find('isgepubliceerd').text
        file['validfrom'] = periode.find('startdatum').text
        file['validthru'] = periode.find('einddatum').text
        index[file['key']] = file
    return index

def sync(kv1index,outputdir):
    newtree = etree.parse(kv1index)
    try:
        oldtree = etree.parse(open(outputdir+'/KV1index.xml'))
        if oldtree.find('mutatiedatum').text == newtree.find('mutatiedatum').text:
            return
        oldindex = parsekv1index(oldtree)
    except Exception as e:
        print e
        oldindex = {}
    newindex = parsekv1index(newtree)
    for key,f in newindex.items():
        if key not in oldindex or newindex[key]['dataownerversion'] > oldindex[key]['dataownerversion']:
            print 'Download file %s version %s' % (f['filename'],str(f['dataownerversion']))
            url = '/'.join(kv1index.strip().split('/')[:-1])+'/'+f['filename']
            downloadfile(outputdir+'/'+f['filename'],url)
    url = '/'.join(kv1index.strip().split('/')[:-1])+'/'+'KV1index.xml'
    downloadfile(outputdir+'/KV1index.xml',url)

def main():
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-s", "--sync", dest="kv1index",
                    action="store",
                    default=False,
                    help="Sync with KV1index feed online")
    parser.add_option("-o", "--output-dir", dest="outputdir",
                    action="store",
                    default='.',
                    help="Directory to store data")
    opts, args = parser.parse_args()
    if opts.kv1index:
        sync(opts.kv1index,opts.outputdir);
if __name__ == '__main__':
    main()
