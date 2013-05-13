# -*- coding: utf-8 -*-

from roo import validators

validators_map = {}


def init():
    items = validators.__dict__
    for name in items:
        if name.startswith('__'):
            continue
        clzz = getattr(validators, name)
        cname = getattr(clzz, 'name', None)
        if cname:
            validators_map[cname] = clzz
    # print validators_map


def addValidator(clzz):
    cname = getattr(clzz, 'name', None)
    if cname:
        validators_map[cname] = clzz

init()
