#!/usr/bin/env python2

'''
txofferfs: A python-fuse filesystem for accessing txoffer DDL listings.

txoffer (https://github.com/Fugiman/txoffer) is a file server with optional XDCC and HTTP
interfaces, the latter of which is what this filesystem uses. This requires that DDLs have been
enabled on the server, which is typically (but not always) the case.

Currently implemented:
- Directory listing.

Currently broken:
- File reading unless the whole file is read at once on the first connection to the server.

Limitations:
- File sizes cannot be known ahead of time (even if packs.txt is enabled on the txoffer server,
  that only lists approximate file sizes) and probing every listed file to get the exact file sizes
  would be exceedingly slow. Such probing (with HTTP HEAD) is done only when reading a file for the
  first time.
- It is impossible to perform multiple concurrent reads on a single file; id est all reads block
  other reads on the same file. Concurrent reads on different files should be fine. (Blame txoffer
  for this; it assumes dumb things if we don't use the same connection for the same file.)
- When this does work (i.e. basically never), the CPU usage is quite high even when just copying a
  file. This is probably not fixable.
'''

from __future__ import print_function

import fuse, stat, errno, codecs, time, posix, random, httplib, sys, threading

from errno import ENOSYS
from HTMLParser import HTMLParser
from fuse import Fuse

fuse.fuse_python_api = (0, 2)

ST_DEV = 0x3c01
# device inode. this has to be unique but the API doesn't enforce that for us (wtf). just cross our
# fingers and hope for the best?

ST_SIZE = 2**30
# default file size to use when we don't know the actual file size (i.e. before the first read).
# we don't want to load all the file sizes at once because that could be slow (and blocking).

CACHE_TIME = 30
# cache time in seconds (TODO: make this an option)

LOG_FILE = open('/tmp/txfs.log', 'a')
def LOG(s, newline=True):
    LOG_FILE.write(s+'\n'*newline)
    LOG_FILE.flush()

HTTP_TIMEOUT = 10
# HTTP timeout in seconds

site = 'txoffer-test'

if site[-1] == '/':
    site = site[:-1]

if ':' in site:
    site_protocol, site = site.split(':', 1)
    site = site[2:]
else:
    site_protocol = 'http'

if site_protocol == 'http':
    Connection = httplib.HTTPConnection
elif site_protocol == 'https':
    Connection = httplib.HTTPSConnection
else:
    sys.exit('unsupported protocol %s' % site_protocol)

# default HTTP headers to use
default_headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0',
                   'Accept' : '*/*',
                   'Host' : site,
                   'Connection' : 'close'
                  }
#default_headers = {'User-Agent': 'txfs/0.2'}

class ConnectionManager():
    """
    halp how does i mutex
    """
    def __init__(self, site):
        self.connections = {}
        self.locks = {}
        self.site = site
        lock = threading.Lock()
        self.acquire, self.release = lock.acquire, lock.release

    def get(self, key):
        self.acquire()
        if key in self.locks:
            lock = self.locks[key]
        else:
            lock = self.locks[key] = threading.Lock()
        self.release()
        lock.acquire()
        LOG('ConnectionManager: acquired lock for key %r' % key)
        self.acquire()
        if key in self.connections:
            conn = self.connections[key]
        else:
            conn = self.connections[key] = Connection(self.site, timeout=HTTP_TIMEOUT)
        self.release()
        return conn

    def unget(self, key):
        self.locks[key].release()
        LOG('ConnectionManager: released lock for key %r' % key)

    def reget(self, key):
        self.connections[key].close() # conn.close is idempotent so it's safe to call it regardless
        conn = self.connections[key] = Connection(self.site, timeout=HTTP_TIMEOUT)
        return conn

def escape(s):
    """Escape a string using percent-encoding."""
    out = ''
    for c in s:
        if '0' <= c <= '9' or 'A' <= c <= 'Z' or 'a' <= c <= 'z' or \
            c in "!$&'()*+,-./:;=?@_~":
            out += c
        else:
            c_utf8 = codecs.encode(c, 'utf-8')
            out += ''.join('%%%2X' % ord(cc) for cc in c_utf8)
    return out

class PacklistParser(HTMLParser):
    """Barebones parser for txoffer's DDL listing."""
    def __init__(self, *args, **kwargs):
        HTMLParser.__init__(self, *args, **kwargs)
        self.listing = False
        self.entries = []
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self.listing:
            if tag == 'a':
                #print('link found; %r' % attrs)
                url = attrs.get('href', '').strip()
                if url == '':
                    return
                self.entries.append(url)
        elif tag == 'ul' and attrs.get('class') == 'list':
            self.listing = True
    def handle_endtag(self, tag):
        if self.listing and tag == 'ul':
            self.listing = False

def gen_pack_dict(entries):
    """
    Generate dicts for the files offered. The first dict returned has the file names (determined 
    by the part of the URL following the last forward slash) as the keys, whereas the second dict
    has the pack numbers as the keys. Both dicts have the URLs as the values.
    
    All keys and values are stored as Unicode strings.
    """
    packs, packs_no = {}, {}
    for url in entries:
        packno, filename = url.split('/')[2:]
        if filename not in packs and filename != 'pack':
            packs[filename] = url
            # we reserve the file name "pack" because we use it to access the files by pack no
        else:
            print('WARNING: duplicate file name (%s) found in pack list' % filename)
        packs_no[packno] = url
    return packs, packs_no

class TxofferFS(Fuse):
    def __init__(self, *args, **kwargs):
        Fuse.__init__(self, *args, **kwargs)
        conn = Connection(site)
        conn.request('GET', '/', headers=default_headers)
        resp = conn.getresponse()
        parser = PacklistParser()
        parser.feed(codecs.decode(resp.read(), 'utf-8', 'ignore'))
        conn.close()
        self.plparser = parser
        self.packs, self.packs_no = gen_pack_dict(parser.entries)
        self.size_cache = {}
        self.mount_time = int(time.time())
        self.conns = ConnectionManager(site)
        # XXX DEBUG
        print('txfs initialised; %d packs' % len(parser.entries))

    '''
    def canonicalise_path(self, path):
        """
        Return the canonical form of a path as a Unicode string. Input is expected to be encoded
        in UTF-8.
        
        / => /
        /pack => /pack
        /pack/123 => /pack/123
        /pack/123/file.mp4 => /pack/123/file.mp4
        /file.mp4 => /pack/123/file.mp4
        
        We do this instead of just symlinking things because then I'd have to write code to handle
        symlinks, and the less filesystem code I write, the better.
        """
        path = codecs.decode(path, 'utf-8', 'ignore')
        if path == '/' or path == '/pack' or path.startswith('/path/'):
            return path
        filename = path[1:]
        if filename in self.packs:
            return self.packs[filename]
    '''

    def getattr(self, path):
        LOG('*** getattr ' + path)
        path_unicode = codecs.decode(path, 'utf-8', 'ignore')
        if path == '/':
            return posix.stat_result((
                stat.S_IFDIR | 0o555, # st_mode
                0xfffffffe, # st_ino
                ST_DEV, # st_dev
                1, # st_nlink
                0, # st_uid
                0, # st_gid
                0, # st_size
                self.mount_time, # st_atime
                self.mount_time, # st_mtime
                self.mount_time, # st_ctime
                ))
        if path == '/pack':
            return posix.stat_result((
                stat.S_IFDIR | 0o555, 0xfffffffd, ST_DEV, 1, 0, 0, 0,
                self.mount_time, self.mount_time, self.mount_time
                ))
        if path.startswith('/pack/'):
            path_parts = path_unicode[6:].split('/')
            if len(path_parts) == 1:
                packno = path_parts[0]
                if packno not in self.packs_no:
                    return -errno.ENOENT
                return posix.stat_result((
                    stat.S_IFDIR | 0o555, 0x80000000 | int(packno), ST_DEV, 1, 0, 0, 0,
                    self.mount_time, self.mount_time, self.mount_time
                    ))
            if len(path_parts) == 2:
                packno, filename = path_parts
                if packno not in self.packs_no or path_unicode != self.packs_no[packno]:
                    return -errno.ENOENT
                return posix.stat_result((
                    stat.S_IFREG | 0o444, int(packno), ST_DEV, 2, 0, 0,
                    self.size_cache.get(path_unicode, ST_SIZE),
                    self.mount_time, self.mount_time, self.mount_time
                    ))
            return -errno.ENOENT
        filename = path_unicode[1:]
        if filename in self.packs:
            url = self.packs[filename] # canonical file name
            packno = url.split('/')[2]
            return posix.stat_result((
                stat.S_IFREG | 0o444, int(packno), ST_DEV, 2, 0, 0,
                self.size_cache.get(url, ST_SIZE),
                self.mount_time, self.mount_time, self.mount_time
                ))
        return -errno.ENOENT

    def readdir(self, path, offset):
        if time.time() - self.mount_time > CACHE_TIME:
            pass
        # TODO: check against mount_time and reload the listing if more than xxx seconds have
        # elapsed since the last time we loaded.
        if path == '/':
            yield fuse.Direntry('pack')
            for filename in self.packs:
                yield fuse.Direntry(codecs.encode(filename, 'utf-8'))
        elif path == '/pack':
            for packno in self.packs_no:
                yield fuse.Direntry(codecs.encode(packno, 'utf-8'))
        elif path.startswith('/pack/'):
            path2 = path[6:]
            if codecs.decode(path2, 'utf-8', 'ignore') in self.packs_no:
                yield fuse.Direntry(codecs.encode(self.packs_no[path2].split('/')[-1], 'utf-8'))
        else:
            pass
            # not a directory, but I have no idea how to raise an error here.
            # we can't just return ENOTDIR because this is an iterator, not a normal function.

    def statfs(self):
        return posix.statvfs_result((
            4096, # f_bsize
            4096, # f_frsize
            1, # f_blocks
            0, # f_bfree
            0, # f_bavail
            len(self.packs_no)*2+2, # f_files
            0, # f_ffree
            0, # f_favail
            0, # f_flag
            255, # f_namemax
            ))
        # the value of f_flag is essentially a black box since Python doesn't expose any of the
        # relevant bitmasks, so we could either hardcode it (lol) or just leave it as 0.

    def get_content_length(self, path, url, conn=None):
        #url = escape(path)
        if conn is None:
            conn = self.conns.get(path)
        for i in range(15):
            try:
                LOG('HEAD %s' % url)
                conn.request('HEAD', url, headers=default_headers)
                resp = conn.getresponse()
                status = resp.status
                LOG('HEAD status %d' % status)
                headers = resp.getheaders()
                body = resp.read()
                LOG('response headers: %r' % headers)
                LOG('response (should be empty): %r' % body)
                if ('connection', 'close') in headers:
                    LOG('server requested connection closure')
                    conn = self.conns.reget(path)
                if status == 200:
                    length = int(dict(resp.getheaders()).get('content-length', 0))
                    self.conns.unget(path)
                    return length
                elif status == 302:
                    pass
                else:
                    LOG('unexpected HTTP status received')
                    self.conns.unget(path)
                    return 0
            except httplib.HTTPException as e:
                LOG('HEAD failed (%r); maybe connection was closed? retrying...' % e)
                conn = self.conns.reget(path)
            except Exception as e:
                LOG('HEAD failed (%r); unknown exception happened' % e)
                conn = self.conns.reget(path)
        LOG('retry limit reached; connection could not be established')
        self.conns.unget(path)
        return 0

    def read(self, path, length, offset):
        LOG('*** read %s %d %d' % (path, length, offset))
        if not path.startswith('/pack/'):
            filename = codecs.decode(path[1:], 'utf-8', 'ignore')
            if filename not in self.packs:
                return -errno.ENOENT
            path = self.packs[filename]
        else:
            path = codecs.decode(path, 'utf-8', 'ignore')
        url = escape(path)
        LOG('URL: %s' % url)

        LOG('cached file size: %s' % str(self.size_cache.get(path, None)))
        if path not in self.size_cache:
            # get the actual file size on the first read; better late than never!
            self.size_cache[path] = self.get_content_length(path, url)
        size = self.size_cache[path]
        lo, hi = max(0, offset), min(offset+length, size)
        if lo >= hi:
            # zero-length request: either the file is zero-length, the file does not exist, or
            # we're asking to read bytes outside the valid range. in the third case we'd like to
            # handle it specially rather than blindly going ahead because I don't know what HTTP
            # says to do with that and I don't care either.
            return ''
        #headers = {'Range': 'bytes=%d-%d' % (lo, hi-1)}
        headers = {'Range': 'bytes=%d-' % lo}
        headers.update(default_headers)

        conn = self.conns.get(path)
        for i in range(15):
            try:
                LOG('GET %s %r' % (url, headers))
                conn.request('GET', url, headers=headers)
                resp = conn.getresponse()
                status = resp.status
                LOG('GET status %d' % resp.status)
                respheaders = resp.getheaders()
                LOG('response headers: %r' % respheaders)
                if ('connection', 'close') in respheaders:
                    LOG('server requested connection closure')
                    conn = self.conns.reget(path)
                if status == 206 or status == 200:
                    # txoffer sometimes responds with 200 OK instead of 206 Partial Content,
                    # which is COMPLETELY WRONG but whatever
                    chunk = resp.read()
                    LOG('bytes read: %d' % len(chunk))
                    self.conns.unget(path)
                    #return chunk # XXX
                    return chunk[:hi-lo]
                data = resp.read()
                LOG('response: %r' % data)
                if status == 404 or status == 410 or status == 416:
                    self.conns.unget(path)
                    return ''
                if status == 302:
                    pass
            except httplib.HTTPException as e:
                LOG('GET failed (%r); maybe connection was closed? retrying...' % e)
                conn = self.conns.reget(path)
            except Exception as e:
                LOG('GET failed (%r); unknown exception happened' % e)
                conn = self.conns.reget(path)
        LOG('retry limit reached; connection could not be established')
        self.conns.unget(path)
        return ''

    '''def release(self, path, flags):
        return -ENOSYS
        # if we want to make connections close when we release the "files", we'd also need to
        # implement reference counting of some sort, so that we don't prematurely close connections
        # just because we have multiple concurrent reads. EDIT: concurrent reads are now impossible.
    '''

    chmod = chown = fsync = link = mkdir = mknod = rename = rmdir = symlink = truncate = unlink = \
    utime = write = lambda self, *args, **kwargs: -errno.EROFS

    def readlink(self, path):
        return -errno.EINVAL

if __name__ == '__main__':
    LOG('mounting %s' % site)
    server = TxofferFS(version='%prog ' + fuse.__version__,
                       usage='Mount a txoffer DDL list (?)')
    server.parse()
    server.main()
