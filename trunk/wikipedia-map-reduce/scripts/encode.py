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

import utils

STATE_HEADER = 0
STATE_REV = 1

def main(output_path):
    writer = utils.Writer(output_path)

    header = ''
    namespaces = set()
    # slurp up everything before the first header 
    for line in sys.stdin:
        if line.strip() == utils.BEGIN_PAGE:
            break
        r = utils.FIND_NS(line)
        if r:
            namespaces.add(r.group(1))
        header += line

    logging.info('namespaces are %s:' % namespaces)

    # process articles
    id = -1
    title = None
    state = STATE_HEADER

    buffer = []

    def flush_buffer():
        for l in buffer:
            writer.send(utils.NO_END, id, l)

    def reset_buffer():
        del(buffer[:])
        # send the first page header...
        buffer.append(utils.XML_HEADER + '\n')
        buffer.append(header)
        buffer.append(utils.BEGIN_PAGE + '\n')

    reset_buffer()

    for line in sys.stdin:
        stripped = line.strip()

        if state == STATE_HEADER:
            if stripped == utils.BEGIN_PAGE: # transition from header to rev
                reset_buffer()
            elif stripped == utils.BEGIN_REV: # transition from header to rev
                assert(title != None)
                assert(id >= 0)
                state = STATE_REV
                buffer.append(line)
                flush_buffer()
            else: # in header
                if id < 0:
                    r = utils.FIND_ID(line)
                    if r:
                        id = int(r.group(1))
                        logging.info('processing article %s: %s' % (id, title))
                if title == None:
                    r = utils.FIND_TITLE(line)
                    if r:
                        title = r.group(1)
                buffer.append(line)
        elif state == STATE_REV:
            writer.send(utils.NO_END, id, line)
            if stripped == utils.END_PAGE: # end of page
                assert(id >= 0)
                writer.send(utils.END_OF_ARTICLE, id, utils.FOOTER)
                id = -1
                title = None
                state = STATE_HEADER
        else:
            assert(False)

    # close files
    writer.send(utils.END_OF_FILE, None, None)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) == 3 and sys.argv[1] == 'current':
        main(sys.argv[2])
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        sys.stderr.write('usage: %s {current} output_file\n' % sys.argv[0])
        sys.exit(1)
