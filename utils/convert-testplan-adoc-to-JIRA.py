#!/usr/bin/env python3
#
# Copyright 2017-2020 GridGain Systems.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sys import argv, exit
import re

if len(argv) < 2:
    print('Usage: ' + argv[0] + ' <file>.adoc')
    exit(0)

# Usage: prepare your test plan as .adoc file
# run this script:
#
# python3 convert-testplan-adoc-to-JIRA.py <GG-XXXXX.adoc>


def process_monospace(s):
    # convert monospaced '`word`' to '{{word}}'
    p = 0
    r = ''
    for c in s:
        if p == 0:
            if c == '`':
                p = 1
                c = '{{'
        elif p == 1:
            if c == '`':
                p = 0
                c = '}}'
        r = r + c
    return r

RE_LINK = re.compile(r'((http|ftp)s?://[^ ]+)')
RE_LINK_WITH_NAME = re.compile(r'(?:((?:http|ftp)s?://[^\[ ]*)\[([^\]]*)\])')


def process_links(r):
    # convert  'http:/blah.com' to '[http://blah.com]' and
    # 'http://blah.com[blah]' to '[blah|http://blah.com]'

    start = 0
    res = RE_LINK.search(r, start)
    while res is not None:
        res0 = RE_LINK_WITH_NAME.search(r, start)
        if res0 is not None:
            res = res0
            link = res.group(1)
            name = res.group(2)
            r0 = r[:res.start(1)] + '[' + name + '|' + link + ']'
            r = r0 + r[res.end(2) + 1:]
            start = len(r0) + 1
        else:
            link = res.group(1)
            r0 = r[:res.start(1)] + '[' + link + ']'
            r = r0 + r[res.end(1) + 1:]
            start = len(r0) + 1
        res = RE_LINK.search(r, start)
    return r

with open(argv[1], 'r') as f:
    p0 = 0    # doc header state
    p1 = 0    # test references state
    p2 = 0
    o = []
    prevpara = False
    blocktype = ''
    p = 0
    for s in f:
        para = True
        s = s.rstrip('\n')

        # skip comments altogether
        if s.startswith('//'):
            continue

        # skip adoc table bounds, JIRA does not need them
        if s.startswith('|==='):
            prevpara = False
            continue

        # escape * in common 'COUNT(*)' expression
        if '(*)' in s:
            s = s.replace('(*)', '(\*)')
        elif '*' in s and p2 == 0:
            # naive replace '*' if it is only one in the line. should handle most cases
            if re.search(r'\*.+\*', s) is None:
                s = s.replace('*', '\*')

        # skip first block header ('=== Test plan summary') and following line (a reference to ticket)
        if s.startswith('=== '):
            if p0 == 0:
                p0 = 1
                prevpara = False
                continue

        if p0 == 1:
            p0 = 0
            prevpara = False
            continue

        # lines after '=== Test reference(s)' are treated like block quote
        if p1 == 1:
            para = False
            if s.strip() == '':
                p1 = 0
                o.append(blocktype)
                blocktype = ''
                o.append('')
                continue

        # process code block
        if p2 > 0:
            para = False
            if p2 == 1:
                if s == '----':
                    p2 = 2
                    s = '{code:' + language + '}'
            elif p2 == 2:
                if s == '----':
                    s = '{code}'
                    p2 = 0

        # test case section header
        if s.startswith('==== '):
            para = False
            s = 'h3. ' + s[5:]

        # table start surely stops paragraph
        if s.startswith('|'):
            para = False

        # single empty line is a para stop
        if s.strip() == '':
            prevpara = False
            para = False

        # either list or header
        if s.startswith('.'):
            if s.strip() == '.':
                o.append(s)
                continue

            para = False
            # list item
            if s[1] == ' ':
                s = '#' + s[1:]
            # sublist item (two levels deep ought to be enough for anybody)
            elif s[1] == '.':
                s = '##' + s[2:]
            else:
                # looks like it is header ...
                # take its first word
                word = s[1:].split(' ')[0].lower()
                # any simple header is h5
                if word != 'test':
                    s = 'h5. ' + s[1:]
                else:
                    # header starting with 'Test' is either 'Test reference(s)' or not
                    word = s[1:].split(' ')[1].lower()
                    if 'reference' in word:
                        # skip header and go to block quote mode until all references processed
                        p1 = 1
                        blocktype = '{quote}'
                        o.append(blocktype)
                        continue
                    else:
                        # otherwise treat it like any other header
                        s = 'h5. ' + s[1:]

        # process code block
        if s.startswith('[source,'):
            para = False
            p2 = 1
            language = s.strip('[').strip(']').split(',')[1]
            continue

        # tabbed section is a block
        if s.lstrip() != s and p2 == 0:
            if p1 == 0:
                p1 = 1
                if len(o) == 0:
                    blocktype = '{noformat}'
                    o.append(blocktype)
                else:
                    if o[len(o) - 1].startswith('h5. '):
                        blocktype = '{panel}'
                        o[len(o) - 1] = '{panel:title=' + o[len(o) - 1][4:].strip() + '|borderStyle=dashed|borderColor=#ccc|titleBGColor=#ffa|bgColor=#eee}'
                    else:
                        blocktype = '{noformat}'
                        o.append(blocktype)

        r = process_monospace(s)
        r = process_links(r)

        # collapse paragraphs
        if len(o) == 0:
            o.append(r)
        else:
            if para and prevpara:
                o[len(o) - 1] += ' ' + r
            else:
                if prevpara:
                    o.append('')
                o.append(r)
        prevpara = para

    for r in o:
        print(r)

