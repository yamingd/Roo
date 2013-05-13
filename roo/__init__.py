# -*- coding: utf-8 -*-


class enum(object):

    def __init__(self, *args):
        for a in args:
            setattr(self, a, a)
