# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import tornado.ioloop
import beanstalkt

from roo.plugin import BasePlugin, plugin
from roo.job import JobEngine


@plugin
class BeanstalkPlugin(BasePlugin):

    """
    config options as:
    beanstalk.host = '127.0.0.1'
    beanstalk.port = 5672
    beanstalk.pools = 5
    """
    name = "beanstalk"

    def __init__(self, application):
        BasePlugin.__init__(self, application)
        self.application = application
        conf = application.settings.beanstalk
        if 'port' not in conf:
            conf.port = 5672
        if 'host' not in conf:
            conf.host = '127.0.0.1'
        if 'pools' not in conf:
            conf.pools = 5
        self.conf = conf
        # used in every request
        self.engine = BeanstalkJobEngine(self.application)
        # used in JobApplication
        application.settings.jobengine = BeanstalkJobEngine
        setattr(application, 'mq', self.engine)

    def on_before(self, controller):
        self.engine.connect()
        setattr(controller, 'mq', self.engine)


class BeanstalkJobEngine(JobEngine):
    client = None

    @property
    def conf(self):
        return self.application.settings.beanstalk

    def connect(self):
        if self.client is None:
            self.client = beanstalkt.Client(
                host=self.conf.host, port=self.conf.port)
            self.client.connect()
            logger.info('BeanstalkEngine connected')

    def close(self):
        try:
            self.client.close(tornado.ioloop.stop)
        except:
            pass

    def send(self, *messages):
        self.connect()
        for message in messages:
            try:
                ttr = message.get('ttr', 60)
                self.client.use(message.queue)
                self.client.put(message.json(), ttr=int(ttr))
            except:
                logger.exception('unexpected error')

    def start(self):
        try:
            self.client = beanstalkt.Client(
                host=self.conf.host, port=self.conf.port)
            self.client.connect(self.listen)
            ioloop = tornado.ioloop.IOLoop.instance()
            ioloop.start()
        except:
            print "Bye bye!"
            self.close()

    def stop(self):
        try:
            self.client.close(tornado.ioloop.stop)
        except:
            pass

    def _executor(self, msg):
        logger.debug(msg)
        self.client.touch(msg['id'])
        content = msg['body']
        try:
            self.on_message(content)
            self.client.delete(msg['id'])
        except:
            logger.exception('unexpected error:')
        finally:
            self.reserve()

    def listen(self):
        logger.info('Beanstalk Engine Start watching:%s' % self.name)
        self.client.watch(self.name)
        self.client.ignore("default")
        self.reserve()

    def reserve(self):
        self.client.reserve(callback=lambda v: self._executor(v))
