# -*- coding: utf-8 -*-


class RowSet(object):

    def __init__(self, items, item_clazz, total=0, limit=0, start=1):
        self.items = map(long, items)
        self.clzz = item_clazz
        self.total = total
        self.item_func = item_clazz.find
        self.limit = limit
        self.start = start
        self._caches = {}

    @property
    def pages(self):
        ps = 0
        if self.limit > 0:
            ps = self.total / self.limit
            if self.total % self.limit > 0:
                ps = ps + 1
        return ps

    def filter(self, ids):
        if ids:
            self.items = list(set(self.items) - set(ids))

    def __len__(self):
        return len(self.items)

    def _litem(self, wid):
        if wid in self._caches:
            return self._caches[wid]
        self._caches[wid] = ret = self.item_func(wid)
        return ret

    def __getitem__(self, index):
        if isinstance(index, int):
            wid = self.items[index]
            return self._litem(wid)
        elif isinstance(index, slice):
            wids = self.items[index]
            rets = []
            for wid in wids:
                rets.append(self._litem(wid))
            return rets

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def __repr__(self):
        return '<RowSet (%s, %s)>' % (self.clzz.__name__, self.items)

    def pop(self):
        if self.items:
            wid = self.items.pop()
            return self._litem(wid)
        return None


class RankSet(object):

    def __init__(self, items, item_clazz, field, limit=10, start=1):
        """
        items : [('1',1),('2',2)]
        """
        self.field = field
        self.items = items
        self.clzz = item_clazz
        self.item_func = item_clazz.find
        self.limit = limit
        self._caches = {}

    def __len__(self):
        return len(self.items)

    def _litem(self, item):
        iid, score = item
        if iid in self._caches:
            return self._caches[iid]
        ret = self.item_func(int(iid))
        setattr(ret, self.field, int(score))
        self._caches[iid] = ret
        return ret

    def __getitem__(self, index):
        if isinstance(index, int):
            item = self.items[index]
            return self._litem(item)
        elif isinstance(index, slice):
            wids = self.items[index]
            rets = []
            for item in wids:
                rets.append(self._litem(item))
            return rets

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def __repr__(self):
        return '<RankSet (%s, %s)>' % (self.clzz.__name__, self.field)

    def pop(self):
        if self.items:
            item = self.items.pop()
            return self._litem(item)
        return None
