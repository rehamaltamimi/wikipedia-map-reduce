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

    python reencode.py

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

BEGIN_PAGE = '<page>'
END_PAGE = '</page>'
FIND_ID = re.compile('<id>(\d+)</id>').search

HEADER = """<?xml version="1.0" encoding="utf-8"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.3/" version="0.3">
"""

FOOTER = """
</mediawiki>
"""

NO_END = 0
END_OF_FILE = 1
END_OF_ARTICLE = 2

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
            

def main(output_path):
    queue = multiprocessing.Queue(MAX_BUFFERS)
    writer = Writer(output_path, queue, 1)
    writer.start()

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
                queue.put((NO_END, id, HEADER))

        buffer += line
        if line.strip() == END_PAGE:
            assert(id >= 0)
            queue.put((END_OF_ARTICLE, id, buffer+ FOOTER))
            buffer = ''
            id = -1
        elif len(buffer) > BUFFER_SIZE:
            queue.put((NO_END, id, buffer))
            buffer = ''

    # close files
    queue.put((END_OF_FILE, None, None))
    writer.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) != 2:
        sys.stderr.write('usage: %s output_file\n' % sys.argv[0])
        sys.exit(1)
    main(sys.argv[1])
