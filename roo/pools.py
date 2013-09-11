# -*- coding: utf-8 -*-
from Queue import Queue
from contextlib import contextmanager


class Pool(Queue):
    def __init__(self, name, args, n_slots=5):
        Queue.__init__(self, n_slots * 2)
        self.name = name
        self.args = args
        for i in xrange(n_slots):
            self.put(self.make_instance())

    @contextmanager
    def reserve(self, timeout=None):
        """Context manager for reserving a client from the pool.
        If *timeout* is given, it specifiecs how long to wait for a client to
        become available.
        """
        if self.empty():
            rc = self.make_instance()
        else:
            rc = self.get(True, timeout=timeout)
        try:
            yield rc
        finally:
            self.put(rc)

    def make_instance(self):
        pass
