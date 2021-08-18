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
        return self.reconnect(cmd=cmd)

    def stop(self, cmd=None):
        if self.sam is not None:
            self.sam.shutdown()
            self.sam = None
            time.sleep(1)
        if cmd is not None:
            cmd.inform('text="hxhal disconnected"')

    def reconnect(self, instrumentName=None, linkType=None, firmwareName=None, configName=None, cmd=None):
        """Establish a new connection to the SAM, and power-cycle and re-initialize the ASIC.

        Args:
        instrumentName : `str`
          The instrument name, for configuration lookup.
        linkType : {'usb', 'gev'}
          The communications link type. Must be 'usb' for H4s
        firmwareName : `str`
          The name of the firmware file. If absolute, a path, else searched in `hxhal` dir
        configName : `str`
          The name of the ASIC configuration. If absolute, a path, else searched in `hxhal` dir
        cmd : the controlling `Command`, if any.
        """

        if cmd is None:
            cmd = self.actor.bcast

        if instrumentName is None:
            instrumentName = self.actor.instrument

        if linkType is not None:
            link = linkType
            samId = None
        else:
            link = self.actor.config.get('hxhal', 'link')
            samId = int(self.actor.config.get('hxhal', 'samId'))
        cmd.inform('text="connecting to instrument=%s link=%s samId=%s"' % (instrumentName, link, samId))

        initArgs = dict()
        if firmwareName is None:
            try:
                firmwareName = self.actor.config.get('hxhal', 'firmware')
                initArgs['asicRegisterFile'] = firmwareName
            except Exception as e:
                self.logger.warn('no firmware config:', e)
                cmd.warning(f'text="no firmware config, using some default: {e}"')
        else:
            initArgs['asicRegisterFile'] = firmwareName

        if configName is None:
            try:
                configName = self.actor.config.get('hxhal', 'hxconfig')
            except Exception as e:
                self.logger.warn('no hxconfig config:', e)
                cmd.warning(f'text="no hxconfig config: {e}"')
                configName = None

        cmd.inform(f'text="power-cycling and configuring SAM and ASIC; link={link} SAM={samId} '
                   f'firmware={firmwareName} config={configName}"')
        try:
            self.sam = samControl.SAM(linkType=link, deviceId=samId,
                                      bouncePower=True,
                                      instrumentName=instrumentName,
                                      logger=None, logLevel=logging.DEBUG,
                                      **initArgs)
        except Exception as e:
            msg = f'failed to open device (link={link}, samId={samId}): {e}'
            raise RuntimeError(msg)

        if configName is not None:
            cmd.inform(f'text="setting ASIC config to {configName}"')
            try:
                self.sam.updateHxRgConfigParameters('h4rgConfig', configName)
            except Exception as e:
                msg = f'failed to configure device (configName={configName}): {e}'
                raise RuntimeError(msg)
