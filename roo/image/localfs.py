# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import os
from PIL import Image

from .base import BaseImageFS
from roo.config import settings


class LocalImageFS(BaseImageFS):

    """
    使用本地硬盘作为存储
    """

    def initfs(self):
        """
        image = {
            "folder":"/www/images/project"
        }
        """
        self.root_folder = settings.image.folder

    def save(self, im, path, quality=85):
        if im.mode != "RGB":
            im = im.convert("RGB")
        if quality:
            im.save(path, quality=quality)
        else:
            im.save(path)
    
    def thumb(self, path, sizes):
        path = os.path.join(self.root_folder, path)
        oimg = Image.open(path)
        ext = self.extname(path)
        for w, h in sizes:
            img2 = self._post_thumb(oimg, w, h)
            spath = '%s.%sx%s%s' % (path, w, h, ext)
            logger.debug('thumb:%s' % spath)
            self.save(img2, spath)

    def crop(self, path, sizes):
        path = os.path.join(self.root_folder, path)
        oimg = Image.open(path)
        ext = self.extname(path)
        cimg = self._post_crop_image(oimg)
        for w, h in sizes:
            img2 = self._post_thumb(cimg, w, h)
            spath = '%s.%sx%s%s' % (path, w, h, ext)
            logger.debug('crop:%s', spath)
            self.save(img2, spath)
