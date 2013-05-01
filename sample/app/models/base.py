# -*- coding: utf-8 -*-
from datetime import datetime

from roo.model import EntityDef, prstat
from roo.plugins.mysql import MySQLModel, MySQLStatModel


class BaseModel(MySQLModel):
    pass


@EntityDef('t_person', 10)
class Person(BaseModel):

    @classmethod
    def new(clz, *args, **kwargs):
        name = args[0]
        sql = "insert into t_person(id, name, create_at) values(%s, %s, %s)"
        id = clz.create(sql, name, datetime.now())
        sql = "insert into t_person_stat(id)values(%s)"
        clz.dbsess().execute(sql, id)
        ret = Person({'id': id, 'name': name})
        return ret

    def update(self, *args, **kwargs):
        sql = "update t_person set real_name = %s where id = %s"
        self.__class__.save(sql, args[0], self.id)

    @classmethod
    def find_byname(clz, name):
        sql = "select id from t_person where name = %s "
        ret = clz.find_one(sql, name)
        if ret:
            ret = clz.find(ret.id)
            return ret
        return None

    @prstat('PersonStat')
    def stat(self):
        pass

    @classmethod
    def auth(clz, id):
        shard = clz.dbm().find(long(id))
        clz.dbm().setdb(shard[0])
        return clz.find(id)


@EntityDef('t_person_stat', 11)
class PersonStat(MySQLStatModel):
    pass
