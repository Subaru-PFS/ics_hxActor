from __future__ import print_function, absolute_import, division
from past.builtins import reload

import logging

import sam.sam as sam
reload(sam)

class hxhal(object):
    def __init__(self, actor, name,
                 loglevel=logging.DEBUG):

        self.actor = actor
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logLevel = loglevel
        self.logger.setLevel(loglevel)

        self.sam = None

    def start(self, cmd=None):
        link = self.actor.config.get('hxhal', 'link')
        samId = int(self.actor.config.get('hxhal', 'samId'))
        self.logger.info('connecting to link:%s samId=%s', link, samId)

        try:
            self.sam = sam.SAM(linkType=link, deviceId=samId,
                               logger=None, logLevel=logging.DEBUG)
        except Exception as e:
            self.actor.bcast.warn('text="failed to open device (link=%s, samId=%s): %s"' %
                                  (link, samId, e))

    def stop(self, cmd=None):
        if self.sam is not None:
            self.sam = None

            
        

    
    
