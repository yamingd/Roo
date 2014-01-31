# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import time

epoch = datetime(1970, 1, 1)


def epoch_seconds(date):
    """Returns the number of seconds from the epoch to date. Should match
    the number returned by the equivalent function in postgres."""
    td = date - epoch
    return td.days * 86400 + td.seconds + (float(td.microseconds) / 1000000)


def tomorrow():
    today = datetime.now()
    yester = today + timedelta(days=1)
    return yester

site_epoch = datetime(2011, 5, 1)


def mksetime(d=datetime.now()):
    td = d - site_epoch
    return td.days * 86400 + td.seconds + (float(td.microseconds) / 1000000)


def now():
    return datetime.now()


def now_epoch():
    return int(time.mktime(datetime.now().timetuple()))


def today():
    dt = datetime.now()
    return datetime(dt.year, dt.month, dt.day)


def today_str():
    return format(today())


def yesterday(days=1):
    dt = datetime.now()
    dt = dt - timedelta(days=days)
    return datetime(dt.year, dt.month, dt.day)


def time_as_epoch(value):
    if isinstance(value, datetime):
        return int(time.mktime(value.timetuple()))
    return None


def epoch_as_time(value):
    return datetime.fromtimestamp(float(value))


def format(date, pattern='%Y%m%d'):
    return date.strftime(pattern)


def as_date(str_date, format=u'%Y-%m-%d %H:%M:%S'):
    if isinstance(str_date, datetime):
        return str_date
    parts = str_date.split('.')
    return datetime.strptime(parts[0], format)


def as_epoch(str_date, format=u'%Y-%m-%d %H:%M:%S'):
    if isinstance(str_date, unicode):
        parts = str_date.split('.')
        dt = datetime.strptime(parts[0], format)
    elif isinstance(str_date, datetime):
        dt = str_date
    else:
        return None
    return int(time.mktime(dt.timetuple()))
