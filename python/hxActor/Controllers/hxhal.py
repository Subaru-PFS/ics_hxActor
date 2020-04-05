from importlib import reload

import logging
import time

from sam import sam as samControl
from sam import logic as samLogic

reload(samControl)
reload(samLogic)

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

        initArgs = dict()
        try:
            firmware = self.actor.config.get('hxhal', 'firmware')
            initArgs['asicRegisterFile'] = firmware
        except Exception as e:
            self.logger.debug('no firmware config:', e)
            pass
        
        try:
            hxconfig = self.actor.config.get('hxhal', 'hxconfig')
        except Exception as e: 
            self.logger.debug('no hxconfig config:', e)
            hxconfig = None

        if cmd is not None:
            cmd.inform(f'text="(re-)starting hxhal; link={link} requiring SAM={samId}, with firmware={firmware}"')            
        try:
            self.sam = samControl.SAM(linkType=link, deviceId=samId,
                                      bouncePower=True,
                                      instrumentName=self.actor.instrument,
                                      logger=None, logLevel=logging.DEBUG,
                                      **initArgs)
        except Exception as e:
            self.actor.bcast.warn('text="failed to open device (link=%s, samId=%s): %s"' %
                                  (link, samId, e))
            return
        if hxconfig is not None:
            if cmd is not None:
                cmd.inform(f'text="setting ASIC config to {hxconfig}"')            
            self.sam.updateHxRgConfigParameters('h4rgConfig', hxconfig)
        
    def stop(self, cmd=None):
        if self.sam is not None:
            self.sam.shutdown()
            self.sam = None
            time.sleep(1)
        if cmd is not None:
            cmd.inform('text="hxhal disconnected"')
