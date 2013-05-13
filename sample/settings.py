# -*- coding: utf-8 -*-
from roo.lib import ODict


#Site Configuration
site = {
    "name": "sample",
    "login_url": "/account/index",
    "gzip": False,
    'logging': 'debug',
    "title": u"测试站点",
    "keywords": u"测试站点",
    "description": u"测试站点",
    "domain": u"http://sample.dev.com",
    "cdn": u"http://sample.dev.com",
    "feedback": u"_site_feedback_mail",
    "webmaster": u"_site_webmaster_mail",
    "admin_mail": u"_site_admin_mail",
    "debug": True,
    "xsrf_cookies": True
}
site = ODict(site)


#Session Security
session = {
    "key": u"sample",
    "cookie_secret": "61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
    "expires_days": 30,
    "cookie_id": "_sess",
    "auth_cookie": "_auth",
    "auth_service": "Person"
}
session = ODict(session)

mail = {
    "engine": "loclmem",
    "user": "user",
    "passwd": "password",
    "host": "host",
    "port": "port",
    "use_tls": True
}
mail = ODict(mail)

mysql = {
    "name": "roo_",
    "user": "quora",
    "passwd": "quora-dev",
    "num_clients": 5,
    "servers": {
        "shard01": ("127.0.0.1:3309", 1, 2)
    },
    "debug": True
}
mysql = ODict(mysql)

cache = {
    "hosts": ["127.0.0.1:11219"],
    "pools": 5
}
cache = ODict(cache)

redis = {
    "host": "127.0.0.1",
    "port": 63791
}
redis = ODict(redis)

beanstalk = {
    "host": "127.0.0.1",
    "port": 11300,
    "pools": 5
}
beanstalk = ODict(beanstalk)

amqp = {
    "host": "192.168.0.104",
    "port": 56720,
    "pools": 5,
    "user": "admin",
    "passwd": "yamingd51",
    "vhost": "/",
    "exchange": "rabbit",
    "ha": True
}
amqp = ODict(amqp)

plugins = ["mail", "cache", "redis", "mysql"]
