# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import tornado.web

import binascii
import uuid
import urlparse
import urllib

from roo import threadlocal
from roo.plugin import BasePlugin, plugin


@plugin
class SessionPlugin(BasePlugin):

    """
    SessionPlugin config items as follows:
    session.cookie_id = $session_id
    session.expires_days = $expires_days
    session.auth_cookie = $auth_cookie_id
    session.auth_service = $auth_service (auth)
    """
    name = "session"

    @property
    def session_config(self):
        return self.config.session

    def gen_session_id(self, controller):
        sessionid = controller.get_cookie(self.session_config.cookie_id)
        if not sessionid:
            sessionid = binascii.b2a_hex(uuid.uuid4().bytes)
            days = self.session_config.expires_days
            controller.set_cookie(
                self.session_config.cookie_id, sessionid, expires_days=days)
        return sessionid

    def on_before(self, controller):
        """
        init user and check user's permission.
        auth_service MUST have a method named 'auth'
        """
        session_id = controller.get_cookie(self.session_config.cookie_id)
        cookie_id = str(self.session_config.auth_cookie)
        userid = controller.get_secure_cookie(cookie_id)
        user = None
        if userid:
            auth_service = self.session_config.auth_service
            auth_service = getattr(self.application.models, auth_service)
            user = auth_service.auth(userid)
            if user:
                if not session_id:
                    session_id = self.gen_session_id(controller)
                    setattr(user, 'just_signin', True)
                    setattr(user, 'session_id', session_id)
                threadlocal.set_user(user)
                
        if not session_id:
            session_id = self.gen_session_id(controller)
        threadlocal.set_sessionid(session_id)
        threadlocal.set_ip(controller.request.remote_ip)
        if session_id:
            controller.set_cookie(self.session_config.cookie_id, session_id)

        if not user and controller.require_auth:
            h = controller.request.headers.get('X-Requested-With', None)
            if h and h == 'XMLHttpRequest':
                raise tornado.web.HTTPError(403, self.__class__.__name__)
            else:
                if controller.request.method in ("GET", "HEAD"):
                    url = controller.get_login_url()
                    if "?" not in url:
                        if urlparse.urlsplit(url).scheme:
                            # if login url is absolute, make next absolute too
                            next_url = controller.request.full_url()
                        else:
                            next_url = controller.request.uri
                        url += "?" + urllib.urlencode(dict(next=next_url))
                    controller.redirect(url)
                else:
                    raise tornado.web.HTTPError(403, self.__class__.__name__)
