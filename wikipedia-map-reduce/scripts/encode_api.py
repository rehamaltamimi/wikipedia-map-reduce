#!/usr/bin/python -O

"""

Author: Shilad Sen

Generates a hadoop-friendly file for a set of articles.
Uses the Wikipedia API to get the most recent information.
The lzma binary be available on the path.

Overview:

    Input: Articles titles, read from sys.argv

    Output:
    One key/value pair per line.
    The key is the article id, and the value is the lzma-compressed article.
    After compression, values of tab, newline, carriage return, and slash are
    escaped before they are written to the file.

Arguments:

    python encode_api.py Squash Chicago

Requires:

    pywikibot: http://www.mediawiki.org/wiki/Manual:Pywikipediabot/Installation

"""


import sys
import logging
import re
import time

from xml.sax.saxutils import escape

if sys.version < '2.6':
    sys.stderr.write('this program requires python version >= 2.6\n')
    sys.exit(1)


import wikipedia

import utils

ONLY_CURRENT = False

STATE_HEADER = 0
STATE_REV = 1

def main(output_path, titles):
    utils.LZMA_ARGS = ['lzma', '-z', '-c', '-']
    writer = utils.Writer(output_path, False)
    for title in titles:
        send_page(title, writer)
    writer.send(utils.END_OF_FILE, None, None)

def getId(page):
    params = {
        'action': 'query',
        'prop': 'info',
        'titles': page.title()
    }

    result = wikipedia.query.GetData(params, page.site())['query']['pages']
    return list(result.keys())[0]

def getRevisions(page, minId, maxId):
    assert(minId <= maxId)
    params = {
        'action': 'query',
        'format': 'xml',
        'prop': ['revisions', 'info'],
        'rvprop' : ['content', 'ids', 'flags', 'timestamp', 'user', 'comment', 'size'],
        'titles': page.title(),
        'rvdir': 'newer',
        'rvstartid' : minId,
        'rvendid' : maxId,
    }

    result = wikipedia.query.GetData(params, page.site())['query']['pages']
    return list(result.values())[0]['revisions']
    

def send_page(title, writer):
    p = wikipedia.Page('en', title)
    id = getId(p)
    writer.send(utils.NO_END, id, utils.HEADER)
    writer.send(utils.NO_END, id, utils.BEGIN_PAGE + '\n')
    writer.send(utils.NO_END, id, '<title>%s</title>\n' % (title))
    writer.send(utils.NO_END, id, '<id>%s</id>\n' % (id))
    writer.send(utils.NO_END, id, '<revisions>\n')
    
    logging.info('getting revisions for page %s id %s' % (title, id)) 
    revInfos = p.getVersionHistory(getAll=True, reverseOrder=True)
    logging.info('found %d revisions for page %s' % (len(revInfos), title))
    i = 0
    while i < len(revInfos):
        slice = revInfos[i:(i+10)]
        minId = slice[0][0]
        maxId = slice[-1][0]
        revisions = getRevisions(p, minId, maxId)
        logging.info('doing revision %d of %d' % (i, len(revInfos)))
        if len(revisions) != len(slice):
            logging.warn('too few revisions!')
        for rev in revisions:
            try:
                write_revision(writer, id, rev)
            except:
                logging.exception('rev %s failed with keys %s' % (id, `rev.keys()`))
        i += 10

    writer.send(utils.NO_END, id, '</revisions>\n')
    writer.send(utils.NO_END, id, utils.END_PAGE + '\n')
    writer.send(utils.END_OF_ARTICLE, id, utils.FOOTER)

def write_revision(writer, id, rev):
    user = escape(rev['user'])
    comment = escape(rev['comment'])
    content = escape(rev['*'])
    timestamp = rev['timestamp']
    revid = rev['revid']
    minor = rev.get('flags')

    writer.send(utils.NO_END, id, '<revision>\n')
    writer.send(utils.NO_END, id, '<id>%s</id>\n' % (revid))
    writer.send(utils.NO_END, id, '<timestamp>%s</timestamp>\n' % (timestamp))
    if is_ip(user):
        writer.send(utils.NO_END, id, '<contributor><ip>%s</ip></contributor>\n' % (user))
    else:
        writer.send(utils.NO_END, id, '<contributor><username>%s</username><id>-1</id></contributor>\n' % (user))
    if minor:
        writer.send(utils.NO_END, id, '<minor>1</minor>\n')
    writer.send(utils.NO_END, id, '<comment>%s</comment>\n' % (comment))
    writer.send(utils.NO_END, id, '<text>%s</text>\n' % (content))
    writer.send(utils.NO_END, id, '</revision>\n')

def is_ip(phrase):
    return re.match('\d+.\d+.\d+.\d+', phrase)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) >= 3:
        main(sys.argv[1], sys.argv[2:])
    else:
        sys.stderr.write('usage: %s output_path title1 title2 ...\n' % sys.argv[0])
        sys.exit(1)
