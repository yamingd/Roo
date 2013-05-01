# -*- coding: utf-8 -*-
from roo import log
logger = log.logger(__name__)

from datetime import datetime

from roo.lib import ODict
from roo.lib import jsonfy
from roo.controller import Controller


class JobEngine(object):

    def __init__(self, application, name='jobs'):
        """
        name is Queue's name
        application is application's context, so can access configuration or models from application
        """
        self.name = name
        self.application = application
        self.debug = application.settings.site.debug
        self.initialize()

    def initialize(self):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    def publish(self, *messages):
        pass

    def on_message(self, raw_data):
        if self.debug:
            logger.debug('[%s] Received: %s' % (self.name, raw_data))
        if not raw_data:
            logger.error('Content is None')
            return
        # parse message as json
        try:
            content = JobMessage.from_json(raw_data)
        except:
            logger.error('json parse content error: %s', raw_data)
            logger.exception('unexpected error:')
            return

        # deal with message
        error = False
        try:
            handle = getattr(self.application, 'handle_message')
            handle(content)
        except BaseException as e:
            error = True
            logger.error('error: %s', raw_data)
            logger.exception('unexpected error:')
            self.on_message_failed(e, raw_data)
        finally:
            if error:
                logger.error('handle message with error.')

    def on_message_failed(self, error, raw_data):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def __del__(self):
        try:
            self.stop()
            self.close()
        except:
            pass

    def newjob(self, handler_url):
        if not handler_url.startswith('/jobs/'):
            handler_url = '/jobs' + handler_url
        return JobMessage(handler_url)


class JobMessage(ODict):

    def __init__(self, handler_url, **kwargs):
        """
        handler_url is something like '/jobs/person/add'
        """
        self['handler_url'] = handler_url
        self['create_at'] = datetime.now()
        self['queue'] = handler_url.split('/')[2]
        self.update(kwargs)

    def json(self):
        return jsonfy.dumps(self)

    @classmethod
    def from_json(clz, data):
        ret = jsonfy.loads(data)
        handler_url = ret.pop('handler_url')
        return JobMessage(handler_url, **ret)

    @property
    def queue(self):
        return self['queue']


class JobMessageHandler(Controller):
    """
    JobMessageHandler base class.
    """
    def _execute(self, transforms, *args, **kwargs):
        """Executes this request with the given output transforms."""
        self._transforms = transforms
        try:
            self.path_args = []
            self.path_kwargs = kwargs
            self.prepare()
            self._execute_method(*self.path_args, **self.path_kwargs)
            self._finished = True
            self.on_finish()
        except Exception as e:
            self._handle_request_exception(e)

