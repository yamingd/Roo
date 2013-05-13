# -*- coding: utf-8 -*-


def _force_unicode(text):
    if not text:
        return u''
    try:
        text = unicode(text, 'utf-8')
    except TypeError:
        try:
            text = unicode(text, 'gbk')
        except:
            text = unicode(text)
    return text


def _force_utf8(text):
    if not text:
        return u''
    return str(_force_unicode(text).encode('utf8'))


def force_bytes(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_bytes, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    if isinstance(s, bytes):
        if encoding == 'utf-8':
            return s
        else:
            return s.decode('utf-8', errors).encode(encoding, errors)
    if strings_only and (s is None or isinstance(s, int)):
        return s

    return s.encode(encoding, errors)
