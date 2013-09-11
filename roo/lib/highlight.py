#!/usr/bin/env python
# -*- coding: utf-8 -*-
from roo import log
logger = log.logger(__name__)

import re
from roo import encoding

keeps = ('em', 'i', 'b', 'strong', 'p', 'br', 'img', 'u')

CLEANBODY_RE = re.compile(r'<(/?)(.+?)>', re.M)
IMG_SRC = re.compile(r'(src="(.*?)")', re.M)
WORD_RE = re.compile(r'(([\s</]*?)(\w+)([\s>]*?))', re.M)


class HtmlLighter(object):

    def __init__(self, domain, keeps=None):
        self.domain = domain
        self.keeps = keeps

    def _format_img(self, ct):
        atts = IMG_SRC.findall(ct)
        if len(atts) == 0:
            return ''
        _, src = atts[0]
        if not src.startswith('http://') and not src.startswith('https://'):
            if not self.domain.endswith('/') and not src.startswith('/'):
                src = '/' + src
            src = u'%s%s' % (self.domain, src)
        return '<img src="%s" />' % src

    def _repl_tags(self, match):
        tag = match.group(2).split(' ')[0].lower()
        # print match.group(0), match.group(1), match.group(2)
        if tag == 'p':
            return '<%sp>' % match.group(1)
        elif tag == 'img':
            # replace src
            ct = match.group(0).lower()
            return self._format_img(ct)
        elif tag in self.keeps:
            return match.group(0).lower()
        return u''

    def strip_tags(self, text):
        text = encoding.force_unicode(text)
        return CLEANBODY_RE.sub(self._repl_tags, text)
    
    def _repl_words(self, match):
        #print match.group(0), match.group(1)
        s = match.group(0)
        #logger.debug('%s, %s' % (s, match.group(1)))
        if s.startswith('<') or s.startswith('>'):
            return s
        return '<u>' + s + '</u>'

    def words(self, text, words):
        if len(words) == 0:
            return text
        text = encoding.force_unicode(text)
        # (\blisp\b)|(\bpython\b)|(\bperl\b)|(\bjava\b)|(\bc\b)
        for w in words:
            restr = '(([</>]*?)\\b%s\\b)' % w.lower()
            #print restr
            regex = re.compile(restr, re.I | re.M)
            text = regex.sub(self._repl_words, text)
        return text
    
    def _repl_sentences(self, match):
        #print match.group(0)
        return '<u>' + match.group(0) + "</u>"

    def sentence(self, text, sentence, limit=4):
        # (\blisp\b)|(\bpython\b)|(\bperl\b)|(\bjava\b)|(\bc\b)
        words = re.split('\s+', sentence)
        if len(words) > limit:
            return text
        text = encoding.force_unicode(text)
        restr = ['(%s)' % w for w in words]
        restr = '(\s*?)'.join(restr)
        restr = '(' + restr + ')'
        #print restr
        regex = re.compile(restr, re.I | re.M)
        text = regex.sub(self._repl_sentences, text)
        return text

    def light(self, text, picks, limit=4):
        if len(picks) == 0:
            return None
        words = [item for item in picks if ' ' not in item]
        sents = [item for item in picks if ' ' in item]
        if len(words) > 0:
            text = self.words(text, words)
        if len(sents) > 0:
            for sent in sents:
                text = self.sentence(text, sent, limit=limit)
        return text

if __name__ == '__main__':
    text = None
    hb = HtmlLighter(u'http://www.51voa.com')
    with open('profile.txt', 'r') as f:
        text = hb.strip_tags(f.read())
    with open('profile.clean.html', 'w+') as f:
        f.write(text.encode('utf8'))
    text2 = hb.light(text, ["News", "strong", "hurricane", "the storm", "according to", "scientists wrote the report"])
    with open('profile.clean2.html', 'w+') as f:
        f.write(text2.encode('utf8'))
