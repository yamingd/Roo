# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import hashlib
import os
import shutil

from PIL import Image

from roo.config import settings


class ImageWrap(object):

    def __init__(self, imageid, fp, file_name):
        self.imageid = imageid
        self.fp = fp
        self.file_name = file_name
        name, pic_ext = os.path.splitext(file_name.encode("utf8"))
        self.image_ext = pic_ext
        self.name = name
        self.path = None

    @property
    def idname(self):
        return str(self.imageid)

    @property
    def image(self):
        return Image.open(self.fp)

    def close(self):
        self.fp.close()


class BaseImageFS(object):

    """
    图片处理、存储基类
    """

    def __init__(self):
        self.initfs()

    def initfs(self):
        """
        在子类实现
        """
        pass
    
    def hexpath(self, file_name):
        root = settings.image.folder
        idhex = hashlib.md5(str(file_name)).hexdigest()
        temp = [idhex[i] + idhex[2] + idhex[4] for i in xrange(0, 24, 6)]
        folder = os.path.join(root, *temp)
        if not os.path.exists(folder):
            os.makedirs(folder)
        return os.path.join(folder, file_name)
    
    def sizepath(self, path, width, height):
        name, ext = os.path.splitext(path)
        return '%s.%sx%s%s' % (path, width, height, ext)

    def move(self, src, dst):
        path = os.path.dirname(dst)
        if not os.path.exists(path):
            os.makedirs(path)
        shutil.move(src, dst)
    
    def extname(self, file_name):
        name, ext = os.path.splitext(file_name)
        return ext

    def save(self, im, path, quality=85):
        """
        在子类实现
        """
        pass

    def remove(self, path, **kwargs):
        try:
            path = os.path.json(settings.image.folder, path)
            os.remove(path)
        except:
            pass

    def thumb(self, path, sizes):
        """
        在子类实现
        """
        pass

    def crop(self, path, sizes):
        """
        在子类实现
        """
        pass

    def _post_thumb(self, img, width, height):
        if not img or not width or not height:
            return img
        """Rescale the given image, optionally cropping it to
        make sure the result image has the specified width and height.
        """
        max_width = float(width)
        max_height = float(height)

        src_width, src_height = img.size
        dst_width, dst_height = max_width, max_height

        r = min(dst_width / src_width, dst_height / src_height)
        if r > 1:
            r = 1
        img2 = img.resize(
            (int(src_width * r), int(src_height * r)), Image.ANTIALIAS)
        return img2

    def _post_crop_image(self, img):
        """
        make the image a square. Crop it.
        """
        src_width, src_height = img.size
        if src_width == src_height:
            return img
        # print src_width,src_height
        if src_width > src_height:
            delta = src_width - src_height
            left = int(delta / 2)
            upper = 0
            right = src_height + left
            lower = src_height
        else:
            delta = src_height - src_width
            left = 0
            upper = int(delta) / 2
            right = src_width
            lower = src_width + upper
        # print left,upper,right,lower
        im = img.crop((left, upper, right, lower))
        return im
