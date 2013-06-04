# -*- coding: utf-8 -*-
import hashlib
import os
import shutil


def hexpath(root, file_name):
    idhex = hashlib.md5(str(file_name)).hexdigest()
    temp = [idhex[i] + idhex[2] + idhex[4] for i in xrange(0, 24, 6)]
    folder = os.path.join(root, *temp)
    if not os.path.exists(folder):
        os.makedirs(folder)
    return folder + '/' + file_name


def move(src, dst):
    path = os.path.dirname(dst)
    if not os.path.exists(path):
        os.makedirs(path)
    shutil.move(src, dst)


def extname(file_name):
    name, ext = os.path.splitext(file_name)
    return ext
