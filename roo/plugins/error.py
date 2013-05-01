# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

from cStringIO import StringIO
import traceback
import sys
from pprint import pprint

from roo.plugin import BasePlugin, plugin


@plugin
class ErrorPlugin(BasePlugin):
    name = "error"

    def on_exception(self, controller, e):
        """
        execute on exception
        """
        err_type, err_val, err_traceback = sys.exc_info()
        # error = u'%s: %s' % (err_type, err_val)
        out = StringIO()
        subject = "%r on %s" % (err_val, controller.request.path)
        print >> out, "TRACEBACK:"
        traceback.print_exception(err_type, err_val, err_traceback, 500, out)
        traceback_formatted = out.getvalue()
        print traceback_formatted
        print >> out, "\nREQUEST ARGUMENTS:"
        arguments = controller.request.arguments
        if arguments.get('password') and arguments['password'][0]:
            password = arguments['password'][0]
            arguments['password'] = password[:2] + '*' * (len(password) - 2)
        pprint(arguments, out)

        print >> out, "\nCOOKIES:"
        for cookie in controller.cookies:
            print >> out, "  %s:" % cookie,
            print >> out, repr(controller.get_secure_cookie(cookie))

        print >> out, "\nREQUEST:"
        for key in ('full_url', 'protocol', 'query', 'remote_ip', 'request_time', 'uri', 'version'):
            print >> out, "  %s:" % key,
            value = getattr(controller.request, key)
            if callable(value):
                try:
                    value = value()
                except:
                    pass
            print >> out, repr(value)

        print >> out, "\nHEADERS:"
        pprint(dict(controller.request.headers), out)

        message = out.getvalue()
        out.close()

        logger.error("%s %s" % (subject, message))
