# -*- coding: utf-8 -*-
from helper import send_email

from roo.mail.backends.console import ConsoleEmailBackend
from roo.mail.backends.locmem import MemEmailBackend
from roo.mail.backends.smtp import SmtpEmailBackend


def build_engine(name):
    if name == 'console':
        return ConsoleEmailBackend()
    elif name == 'locmem':
        return MemEmailBackend()
    elif name == 'smtp':
        return SmtpEmailBackend()
