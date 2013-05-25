# -*- coding: utf-8 -*-


class enum(object):

    def __init__(self, *args):
        self.sets = args
        for a in args:
            if isinstance(a, tuple):
                setattr(self, a[0], a[1])
            else:
                setattr(self, a, a)
