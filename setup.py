#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import distutils.core
import sys
# Importing setuptools adds some features like "setup.py develop", but
# it's optional so swallow the error if it's not there.
try:
    import setuptools
except ImportError:
    pass

kwargs = {}

# Build the epoll extension for Linux systems with Python < 2.6
extensions = []
major, minor = sys.version_info[:2]
python_26 = (major > 2 or (major == 2 and minor >= 6))

version = "0.3"

if major >= 3:
    import setuptools  # setuptools is required for use_2to3
    kwargs["use_2to3"] = True

distutils.core.setup(
    name="roo",
    version=version,
    packages = setuptools.find_packages(exclude=["3rd","demo","*.pyc",".pyo"]),
    package_data = {
        
    },
    install_requires=[
        "tornado",
        "simplejson",
        "pika",
        "pycurl",
        "twisted",
        "pylibmc",
        "redis",
        "beanstalkt",
        "requests",
        "ua-parser",
        "user-agents",
        "pil",
        "txMySQL",
        "txRedis"
    ],
    ext_modules = extensions,
    author="yaming_deng",
    author_email="yamingdeng@gmail.com",
    url="",
    download_url="",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="",
    **kwargs
)
