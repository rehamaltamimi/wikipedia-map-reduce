#!/usr/bin/python -O

"""

Author: Shilad Sen

Converts a wikipedia full-revision-text database dump into Hadoop-friendly files
The lzma binary be available on the path.

Overview:

    Input: Articles with full revision text, read from standard input.

    Output:
    A set of n output files, where file i contains articles whose id % n == i.
    Each output file contains one key/value pair per line.
    The key is the article id, and the value is the lzma-compressed article.
    After compression, values of tab, newline, carriage return, and slash are
    escaped before they are written to the file.

Arguments:

    python split.py numSplits prefix bufferInMBs

    - numSplits is the number of output files the input is split into.
    - prefix is the prefix for each split (i: {0 <= i < n} is appended to them).
    - bufferInMBs is the number of MBs of buffer the program should use, and
    - controls the parallelism of the program.

    A typical usage might look like:

        7za e -so ./enwiki-20100312-pages-meta-history.xml.7z | \
        python ./split.py 100 ./dump-2010-03-12/wikipedia.txt. 5000

"""


import sys

if sys.version < '2.6':
    sys.stderr.write('this program requires python version >= 2.6\n')
    sys.exit(1)

import logging
import math
import multiprocessing
import os
import re
import subprocess
import time


BEGIN_PAGE = '<page>'
END_PAGE = '</page>'
FIND_ID = re.compile('<id>(\d+)</id>').search

HEADER = """<?xml version="1.0" encoding="utf-8"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.3/" version="0.3">
"""

FOOTER = """
</mediawiki>
"""

NUM_SPLITS = 100


NO_END = 0
END_OF_FILE = 1
END_OF_ARTICLE = 2

BUFFER_SIZE = 2**20     # 1M per record sent to child
MAX_BUFFERS = 10000     # Maximum number of buffers that are allowed across all processes


class Writer(multiprocessing.Process):
    """
        A process responsible for writing articles to a single output file.
    """
    def __init__(self, path, q, id):
        multiprocessing.Process.__init__(self)
        self.path = path
        open(path, 'w').close() # truncate
        self.id = id
        self.q = q
    
    def run(self):
        compressor = None
        while True:
            (flag, id, page)  = self.q.get()
            if flag == END_OF_FILE:
                return
            logging.debug('writer %s received %s bytes' % (self.id, len(page)))

            # launch the compressor if necessary
            # it will only be necessary at the beginning of each article
            if not compressor:
                self.write('%s\t' % id)
                (r, w) = os.pipe()
                compressor = Compressor(self.path, (r, w), self.id)
                compressor.start()
                compressor.w = w
                os.close(r)

            # write to the compressor (who writes to the file).
            logging.debug('writer %s about to write %s bytes' % (self.id, len(page)))
            remaining = page
            while remaining:
                n = os.write(compressor.w, remaining)
                assert(n >= 0)
                remaining = remaining[n:]

            logging.debug('writer %s finished bytes' % (self.id))

            # close the compressor and advance the record
            if flag == END_OF_ARTICLE:
                logging.debug('writer %s waiting for compressor' % (self.id))
                os.close(compressor.w)
                compressor.join()
                compressor = None
                logging.debug('writer %s reaped compressor' % (self.id))
                self.write('\n')

    def write(self, data):
        f = open(self.path, 'a')
        f.write(data)
        f.close()


class Compressor(multiprocessing.Process):
    """
        Compresses some bytes (read through childIn),
        Escapes them, and writes them to the path.
    """
    def __init__(self, path, (childIn, childOut), id):
        multiprocessing.Process.__init__(self)
        self.id = id
        self.path = path
        self.childIn = childIn
        self.childOut = childOut

    def run(self):
        logging.debug('compressor %s spawning lzma' % (self.id))
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
            logging.debug('compressor %s read %s bytes' % (self.id, len(bytes)))
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
            

def main(prefix, numSplits):
    global i

    nDigits = math.ceil(math.log10(numSplits))
    format = '%%s%%.%dd' % nDigits
    writers = []
    queues = []
    for i in xrange(numSplits):
        q = multiprocessing.Queue()
        w = Writer(format % (prefix, i), q, str(i))
        w.start()
        writers.append(w)
        queues.append(q)

    i = 0
    def waitForSpace():
        global i

        while sum([q.qsize() for q in queues]) >= MAX_BUFFERS:
            time.sleep(0.1)
        i += 1
        if i % 1000 == 0:
            logging.info('number of used 1MB buffers: %s' % sum([q.qsize() for q in queues]) )

    buffer = ''
    # slurp up everything before the first header 
    for line in sys.stdin:
        if line.strip() == BEGIN_PAGE:
            buffer = line
            break

    # process articles
    id = -1
    for line in sys.stdin:
        if id < 0:
            r = FIND_ID(line)
            if r:
                id = int(r.group(1))
                logging.info('processing article %s' % id)
                waitForSpace()
                queues[id % len(queues)].put((NO_END, id, HEADER))

        buffer += line
        if line.strip() == END_PAGE:
            assert(id >= 0)
            waitForSpace()
            queues[id % len(queues)].put((END_OF_ARTICLE, id, buffer+ FOOTER))
            buffer = ''
            id = -1
        elif len(buffer) > BUFFER_SIZE:
            waitForSpace()
            queues[id % len(queues)].put((NO_END, id, buffer))
            buffer = ''

    # close files
    for q in queues:
        q.put((END_OF_FILE, None, None))
    for w in writers:
        w.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) != 4:
        sys.stderr.write('usage: %s num_splits prefix maxMB< page-meta-history.xml\n' % sys.argv[0])
        sys.exit(1)
    numSplits = int(sys.argv[1])
    prefix = sys.argv[2]
    MAX_BUFFERS = int(sys.argv[3])
    main(prefix, numSplits)
