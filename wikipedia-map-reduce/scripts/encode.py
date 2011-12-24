#!/usr/bin/python -O

"""

Author: Shilad Sen

Converts a wikipedia full-revision-text database dump into a hadoop-friendly file
The lzma binary be available on the path.

Overview:

    Input: Articles with full revision text, read from standard input.

    Output:
    One key/value pair per line.
    The key is the article id, and the value is the lzma-compressed article.
    After compression, values of tab, newline, carriage return, and slash are
    escaped before they are written to the file.

Arguments:

    python encode.py foo.hadoop.txt

        or

    python encode.py current foo.hadoop.txt     # current revision only

    A typical usage might look like:

        7za e -so ./enwiki-20100312-pages-meta-history.xml.7z | \
        python ./reencode.py foo.hadoop.txt
"""


import sys

if sys.version < '2.6':
    sys.stderr.write('this program requires python version >= 2.6\n')
    sys.exit(1)

import multiprocessing
import logging
import math
import os
import re
import subprocess
import time

BUFFER_SIZE = 2**20     # 1M per record sent to child
MAX_BUFFERS = 500       # Maximum number of buffers that are allowed across all processes

ONLY_CURRENT = False
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
    def __init__(self, path):
        self.path = path
        open(path, 'w').close() # truncate
        self.compressor = None
    
    def send(self, flag, id, page):
        if flag == END_OF_FILE:
            return
        logging.debug('writer received %s bytes' % len(page))

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
        #print 'writing', page
        while remaining:
            n = os.write(self.compressor.w, remaining)
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
                        args=['lzma', 'e', '-si', '-so'],
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
            

STATE_HEADER = 0
STATE_REV = 1

def main(output_path):
    writer = Writer(output_path)

    state = STATE_HEADER
    buff = ''
    namespaces = set()
    # slurp up everything before the first header 
    for line in sys.stdin:
        if line.strip() == BEGIN_PAGE:
            buff = line
            break
        r = FIND_NS(line)
        if r:
            namespaces.add(r.group(1))

    logging.info('namespaces are %s:' % namespaces)

    # process articles
    id = -1
    title = None
    buff = ''

    for line in sys.stdin:
        stripped = line.strip()

        if state == STATE_HEADER and stripped == BEGIN_REV: # transition from header to rev
            if buff:
                writer.send(NO_END, id, buff)
            state = STATE_REV
            buff = line
        elif state == STATE_HEADER: # in header
            if id < 0:
                r = FIND_ID(line)
                if r:
                    id = int(r.group(1))
                    logging.info('processing article %s: %s' % (id, title))
                    writer.send(NO_END, id, HEADER)
            if title == None:
                r = FIND_TITLE(line)
                if r:
                    title = r.group(1)
            buff += line
        elif state == STATE_REV and stripped == BEGIN_REV: # new rev, but non the first
            if not ONLY_CURRENT: writer.send(NO_END, id, buff)
            buff = line
        elif state == STATE_REV and stripped == END_PAGE: # end of page
            assert(id >= 0)
            writer.send(END_OF_ARTICLE, id, buff + FOOTER)
            buff = ''
            id = -1
            title = None
            state = STATE_HEADER
        elif state == STATE_REV: # in some rev
            buff += line
        else:
            assert(False)

    # close files
    writer.send(END_OF_FILE, None, None)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) == 3 and sys.argv[1] == 'current':
        ONLY_CURRENT = True
        main(sys.argv[2])
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        sys.stderr.write('usage: %s {current} output_file\n' % sys.argv[0])
        sys.exit(1)
