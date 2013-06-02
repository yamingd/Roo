# -*- coding: utf-8 -*-

import jsonfy
from datetime import datetime


class Article(dict):
    pass


item = Article({'title': u'title', 'uid': 2333, 'create_at': datetime.now()})
jstr1 = jsonfy.dumps(item)
items = [item, item, item]
jstr2 = jsonfy.dumps(items)

print jstr1
print jstr2

item = jsonfy.loads(jstr1)
print item.__class__

item = jsonfy.loads(jstr2)
print item[0].__class__

item = jsonfy.loads('{"doc_type": "user", "client_ip": null, "name": "admin", "roles": ["Administrator"], "create_at": "2013-05-15 14:02:09.156774", "real_name": "Administrator", "hash_passwd": "2c88e4ad58c50c1fefe2dd76c7a166c96ee247ee", "id": 6}')
print item
