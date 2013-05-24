# -*- coding: utf-8 -*-
from roo.lib import jsonfy


class RowSet(object):

    def __init__(self, items, item_clazz, total=0, limit=0, start=1, fmap=long, extras={}):
        self.items = map(fmap, items) if fmap else items
        self.clzz = item_clazz
        self.total = total
        self.item_func = item_clazz.find
        self.limit = limit
        self.start = start
        self.extras = extras
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
        ret = self.item_func(wid)
        if wid in self.extras:
            m = self.extras[wid]
            for name in m:
                setattr(ret, name, m[name])
        self._caches[wid] = ret
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
    
    def as_map(self, fmap=None):
        m = {}
        m['total'] = self.total
        items = []
        for id in self.items:
            item = self._litem(id)
            if fmap:
                item = fmap(item)
            items.append(item)
        m['items'] = items
        return m

    def as_json(self, fmap=None):
        return jsonfy.dumps(self.as_map(fmap=fmap))


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


class StatCollection(object):

    def __init__(self, result, item_clazz=None):
        self.total_rows = 0
        self.keys = []
        self.rows = []
        self.item_clazz = item_clazz
        for item in result:
            self.keys.append(item.key)
            self.rows.append(StatItem(item, item_clazz))
        if self.total_rows == 1 and len(self.rows) > 1:
            self.total_rows = len(self.rows)

    def __getitem__(self, index):
        if index < 0 or index >= len(self.rows):
            raise Exception(
                "index is out of range, min=0, max=" + str(len(self.rows)))
        return self.rows[index]

    def next(self):
        try:
            return self.rows.pop(0)
        except IndexError:
            raise StopIteration


class StatItem(object):

    def __init__(self, item, item_clazz):
        """
        item.value would be a dict.
        """
        self.key = item.key
        self.stat = item.value
        self.item_clazz = item_clazz

    def __getattr__(self, key):
        try:
            return self.stat[key]
        except:
            return 0
