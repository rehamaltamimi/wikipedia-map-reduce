import multiprocessing
import logging
import os
import re
import subprocess
import sys

LZMA_ARGS = ['lzma', 'e', '-si', '-so']


BEGIN_PAGE = '<page>'
END_PAGE = '</page>'
BEGIN_REV = '<revision>'
FIND_ID = re.compile('<id>(\d+)</id>').search
FIND_TITLE = re.compile('<title>([^<]+)</title>').search
FIND_NS = re.compile('<namespace [^>]+>([^<]+)</namespace>').search

HEADER = """<?xml version="1.0" encoding="utf-8"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.3/" version="0.3">
"""

FOOTER = """
</mediawiki>
"""


NO_END = 0
END_OF_FILE = 1
END_OF_ARTICLE = 2

class Writer:
    """
        A process responsible for writing articles to a single output file.
    """
    def __init__(self, path, debug=False):
        self.path = path
        self.debug_path = path + '.debug'
        open(path, 'w').close() # truncate
        self.compressor = None
        self.debug = None
        if debug:
            self.debug = open(self.debug_path, 'w')
    
    def send(self, flag, id, page):
        if flag == END_OF_FILE:
            if self.debug:
                self.debug.close()
                self.debug = None
            return
        logging.debug('writer received %s bytes' % len(page))
        if self.debug:
            self.debug.write(page.encode('UTF-8'))

        # launch the compressor if necessary
        # it will only be necessary at the beginning of each article
        if not self.compressor:
            self.real_write('%s\t' % id)
            (r, w) = os.pipe()
            self.compressor = Compressor(self.path, (r, w))
            self.compressor.start()
            self.compressor.w = w
            os.close(r)

        # write to the compressor (who writes to the file).
        logging.debug('writer about to write %s bytes' % len(page))
        remaining = page
        while remaining:
            n = os.write(self.compressor.w, remaining.encode('UTF-8'))
            assert(n >= 0)
            remaining = remaining[n:]

        logging.debug('writer finished bytes')

        # close the compressor and advance the record
        if flag == END_OF_ARTICLE:
            logging.debug('writer waiting for compressor')
            os.close(self.compressor.w)
            self.compressor.join()
            self.compressor = None
            logging.debug('writer reaped compressor')
            self.real_write('\n')

    def real_write(self, data):
        f = open(self.path, 'a')
        f.write(data)
        f.close()


class Compressor(multiprocessing.Process):
    """
        Compresses some bytes (read through childIn),
        Escapes them, and writes them to the path.
    """
    def __init__(self, path, (childIn, childOut)):
        multiprocessing.Process.__init__(self)
        self.path = path
        self.childIn = childIn
        self.childOut = childOut

    def run(self):
        logging.debug('compressor spawning lzma')
        lzmaLog = open('lzma.log', 'a')
        lzma = subprocess.Popen(
                        args=LZMA_ARGS,
                        stdin=self.childIn,
                        stdout=subprocess.PIPE,
                        stderr=lzmaLog,
                        close_fds=True
                    )
        os.close(self.childOut)
        os.close(self.childIn)
        f = open(self.path, 'a')
        n = 0
        while 1:  
            bytes = lzma.stdout.read(2**16)
            logging.debug('compressor read %s bytes' % len(bytes))
            if not bytes:
                break
            n += len(bytes)
            bytes = bytes.replace('\\', '\\\\')
            bytes = bytes.replace('\n', '\\n')
            bytes = bytes.replace('\r', '\\r')
            bytes = bytes.replace('\t', '\\t')
            f.write(bytes)
        f.flush()
        f.close()
        lzma.wait()
        lzmaLog.close()
        if lzma.returncode != 0:
            logging.error('return code for lzma was %s!' % lzma.returncode)
            

