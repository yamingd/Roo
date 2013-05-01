# -*- coding: utf-8 -*-
from threading import local

_threadlocals = local()


def get_request():
    return get('request', None)


def set_request(req):
    set('request', req)


def get_application():
    return get('app', None)


def set_application(app):
    set('app', app)


def get_user():
    return get('user', None)


def set_user(user):
    set('user', user)


def get_sessionid():
    return get('sessionid', None)


def set_sessionid(sessionid):
    set('sessionid', sessionid)


def get_ip():
    return get('ip', None)


def set_ip(ip):
    set('ip', ip)
    

def set(key, var):
    setattr(_threadlocals, key, var)


def get(key, default=None):
    return getattr(_threadlocals, key, default)

