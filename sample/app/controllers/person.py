# -*- coding: utf-8 -*-
from roo import log
logger = log.logger(__name__)

from datetime import datetime
from roo.router import route

from base import BaseController


@route('/person/(?P<action>[a-zA-Z]+)')
class Person(BaseController):

    def index(self):
        self.cache.set('person.index', 22)
        self.redis.incr('person.index', 10)
        self.xrender()

    def say(self):
        job = self.mq.newjob('/jobs/person/add')
        job['data'] = 'say'
        job['index1'] = self.cache.get('person.index')
        job['index2'] = self.redis.get('person.index')
        self.write("hello world. %s" % job.json())
        self.mq.publish(job)

    def new(self):
        name = self.get_argument('name')
        id = self.models.Person.new(name)
        self.write("save done! id= %s, name=%s" % (id, name))

    def view(self):
        name = self.get_argument('name')
        person = self.models.Person.find_byname(name)
        person.stat().incrby('total_view', 1)
        self.write(person.as_json())

    def remove(self):
        self.mysql.setdb(1)
        name = self.get_argument('name')
        person = self.models.Person.find_byname(name)
        self.models.Person.remove(person.id)
        self.write('delete person with name:' + name)

    def update(self):
        self.mysql.setdb(1)
        name = self.get_argument('name')
        person = self.models.Person.find_byname(name)
        person.update(u'测试名字')

    def stat(self):
        self.mysql.setdb(1)
        name = self.get_argument('name')
        person = self.models.Person.find_byname(name)
        if person:
            person.stat().incrby('total_view', 1)
            self.write(person.stat().as_json())
            person.stat().sync_todb(datetime.now())
        else:
            self.write("can't find Person with name: " + name)
