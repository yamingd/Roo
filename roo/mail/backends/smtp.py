# -*- coding: utf-8 -*-
from roo import log
logger = log.logger(__name__)

import smtplib
import socket
import threading

from roo.config import settings
from roo.mail.dns_name import DNS_NAME
from roo.mail.backends import BaseEmailBackend


class SmtpEmailBackend(BaseEmailBackend):
    name = "smtp"
    
    """
    A wrapper that manages the SMTP network connection.
    """
    def __init__(self, fail_silently=False, **kwargs):
        """
        conf => {'host':'','port':25,'user':'','passwd':'','use_tls':True}
        """
        super(BaseEmailBackend, self).__init__(fail_silently=fail_silently)
        self.conf = settings.mail
        self.host = self.conf.host
        self.port = self.conf.port
        self.username = self.conf.user
        self.password = self.conf.passwd
        self.use_tls = self.conf.use_tls
        self.connection = None
        self._lock = threading.RLock()
        log.info('init EmailBackend')

    def open(self):
        """
        Ensures we have a connection to the email server. Returns whether or
        not a new connection was required (True or False).
        """
        if self.connection:
            # Nothing to do if the connection is already open.
            return False
        try:
            # If local_hostname is not specified, socket.getfqdn() gets used.
            # For performance, we use the cached FQDN for local_hostname.
            self.connection = smtplib.SMTP(self.host, self.port,
                                           local_hostname=DNS_NAME.get_fqdn())
            if self.use_tls:
                self.connection.ehlo()
                self.connection.starttls()
                self.connection.ehlo()
            if self.username and self.password:
                self.connection.login(self.username, self.password)
            return True
        except:
            if not self.fail_silently:
                raise

    def close(self):
        """Closes the connection to the email server."""
        try:
            try:
                self.connection.quit()
            except socket.sslerror:
                # This happens when calling quit() on a TLS connection
                # sometimes.
                self.connection.close()
            except:
                if self.fail_silently:
                    return
                raise
        finally:
            self.connection = None

    def send_messages(self, email_messages):
        """
        Sends one or more EmailMessage objects and returns the number of email
        messages sent.
        """
        if not email_messages:
            return
        self._lock.acquire()
        try:
            new_conn_created = self.open()
            if not self.connection:
                # We failed silently on open().
                # Trying to send would be pointless.
                return
            num_sent = 0
            for message in email_messages:
                sent = self._send(message)
                if sent:
                    num_sent += 1
            if new_conn_created:
                self.close()
        finally:
            self._lock.release()
        return num_sent

    def _send(self, email_message):
        """A helper method that does the actual sending."""
        if not email_message.recipients():
            return False
        try:
            self.connection.sendmail(email_message.from_email,
                                     email_message.recipients(),
                                     email_message.message().as_string())
        except:
            if not self.fail_silently:
                raise
            return False
        return True

