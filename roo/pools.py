# -*- coding: utf-8 -*-
import copy
from Queue import Queue
from contextlib import contextmanager


class ConnectionPool(Queue):
    def __init__(self, rc, n_slots=5):
        Queue.__init__(self, n_slots)
        if rc is not None:
            self.fill(rc, n_slots)

    @contextmanager
    def reserve(self, timeout=None):
        """Context manager for reserving a client from the pool.
        If *timeout* is given, it specifiecs how long to wait for a client to
        become available.
        """
        rc = self.get(True, timeout=timeout)
        try:
            yield rc
        finally:
            self.put(rc)

    def fill(self, rc, n_slots):
        """Fill *n_slots* of the pool with clones of *mc*."""
        for i in xrange(n_slots):
            self.put(copy.copy(rc))
