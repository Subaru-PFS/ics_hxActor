from importlib import reload

import logging
import time

from sam import sam as samControl
from sam import logic as samLogic

reload(samControl)
reload(samLogic)

class DaqState(object):
    def __init__(self):
        self.isValid = False
        self.hxConfig = dict()
        self.spiRegisters = dict()
        self.voltageSettings = dict()
        self.voltageReadings = dict()

class hxhal(object):
    def __init__(self, actor, name,
                 loglevel=logging.DEBUG):

        self.actor = actor
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logLevel = loglevel
        self.logger.setLevel(loglevel)

        self.sam = None

        self.daqState = DaqState()

    def start(self, cmd=None):
        return self.connect(cmd=cmd)

    def stop(self, cmd=None):
        if self.sam is not None:
            self.sam.shutdown()
            self.sam = None
            time.sleep(1)
        if cmd is not None:
            cmd.inform('text="hxhal disconnected"')

    def connect(self, instrumentName=None, linkType=None, cmd=None):
        """Establish a new connection to the SAM. Do *not* initialize any part of it.

        Args:
        instrumentName : `str`
          The instrument name, for configuration lookup.
        linkType : {'usb', 'gev'}
          The communications link type. Must be 'usb' for H4s
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
        self.sam = samControl.SAM(linkType=link, deviceId=samId,
                                  bouncePower=False,
                                  jadeRegisterFile=None,
                                  asicRegisterFile=None,
                                  instrumentName=instrumentName,
                                  logger=None, logLevel=logging.DEBUG)

        # Check to see whether the SAM has been initialized...
        magic = self.sam.link.ReadJadeReg(0xa4)
        if magic != 0xff:
            cmd.fail('text="newly connected SAM does not have register 0xa4 == 0xff. '
                     'SAM is connected but not initialized: consider reconnect bouncePower=True"')
            return

        asics = self.sam.getAvailableAsics(forceGood=False)
        if len(asics) != 1 or asics[0] != 0:
            cmd.fail('text="SAM cannot find single ASIC. '
                     'SAM is connected but not initialized: consider reconnect bouncePower=True"')
            return

        cmd.inform('text="connected to ASIC; updating status"')
        self.grabAllH4Info()

    def reconnect(self, bouncePower=False,
                  instrumentName=None, linkType=None, firmwareName=None, configName=None,
                  cmd=None):
        """Establish a new connection to the SAM, and optionally power-cycle and re-initialize the ASIC.

        By default the DAQ is *not* power-cycled or re-initialized: we assume it is working.

        Args:
        -----
        bouncePower : `bool`
          Whether to fully initialize system, by power-cycling ASIC.
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

        initArgs = dict()

        # Entirely reset ASIC if asked
        if bouncePower:
            if instrumentName is None:
                instrumentName = self.actor.instrument

            if linkType is None:
                linkType = self.actor.config.get('hxhal', 'link')
                samId = int(self.actor.config.get('hxhal', 'samId'))
            else:
                samId = None

            if firmwareName is None:
                try:
                    firmwareName = self.actor.config.get('hxhal', 'firmware')
                    initArgs['asicRegisterFile'] = firmwareName
                except Exception as e:
                    self.logger.warn('no firmware config:', e)
                    cmd.warn(f'text="no firmware config, using some default: {e}"')
            else:
                initArgs['asicRegisterFile'] = firmwareName

            if configName is None:
                try:
                    configName = self.actor.config.get('hxhal', 'hxconfig')
                except Exception as e:
                    self.logger.warn('no hxconfig config:', e)
                    cmd.warn(f'text="no hxconfig config: {e}"')
                    configName = None

            cmd.warn(f'text="power-cycling and configuring SAM and ASIC; link={linkType}'
                     f'firmware={firmwareName} config={configName}"')
            self.sam.powerDownAsic()

            try:
                self.sam = samControl.SAM(linkType=linkType, deviceId=samId,
                                          bouncePower=True,
                                          instrumentName=instrumentName,
                                          logger=None, logLevel=logging.DEBUG,
                                          **initArgs)
            except Exception as e:
                msg = f'failed to open device (link={linkType}, samId={samId}): {e}'
                raise RuntimeError(msg)
        else:
            # Establish minimal connection to SAM. Only configure if explicitly asked to
            #
            self.connect(instrumentName=instrumentName, linkType=linkType,
                         cmd=cmd)

            # Only download configure if asked to. On this path, never power-cycle.
            if firmwareName is not None:
                cmd.inform(f'text="loading ASIC image: {firmwareName}"')
                try:
                    self.sam.initAsic(firmwareName)
                except Exception as e:
                    cmd.fail(f'text="failed to download ASIC image ({firmwareName}): {e}"')

            if configName is not None:
                cmd.inform(f'text="setting ASIC config to {configName}"')
                try:
                    self.sam.updateHxRgConfigParameters('h4rgConfig', configName)
                except Exception as e:
                    cmd.fail(f'text="failed to configure device (configName={configName}): {e}"')

    def grabAllH4Info(self):
        """Gather configuration and voltage info from the DAQ.

        More specifically:
        - the HxRG configuration dictionary built from the ASIC registers.
        - the bias voltage *settings* -- these are fast register readbacks
        - a few bias voltage readings -- these are too expensive to get all.
        - the ROIC SPI registers.
        """
        self.sam.getHxRGConfigParameters()
        self.daqState.hxConfig = self.sam.hxrgDetectorConfig.copy()
        self.daqState.spiRegisters = self.sam.readAllH4SpiRegs()

    def sampleVoltage(self, voltageName):
        sam = self.sam

        try:
            reading, raw = sam.sampleVoltage(voltageName)
        except Exception as e:
            raise RuntimeError('Failed to sample voltage %s: %s"' % (voltageName, e))

        setting = sam.getBiasVoltage(voltageName)

        return setting, reading, raw

    def getVoltageSettings(self):
        """Return a dictionary of all bias voltage settings.

        Also updates self.daqState.voltageSettings
        """

        vlist = self.sam.getBiasVoltages()
        settings = self.daqState.voltageSettings = dict()
        for name, setting in vlist:
            settings[name] = setting

        return settings

    def getMainVoltages(self, doRef=False):
        if doRef:
            self.getRefCal()
        if self.daqState.voltageSettings is None:
            self.getVoltageSettings()

        readings = self.daqState.voltageReadings = dict()
        ret = dict()
        for vname in ('VReset', 'DSub', 'VBiasGate', 'Vrefmain'):
            setting, reading, raw = self.sampleVoltage(vname)
            readings[vname] = reading
            ret[vname] = setting, reading, raw

        return ret

    def getRefCal(self):
        """Sample the ASIC reference offset and gain. """

        self.sam._buildVoltageTable()
        aduPerVolt, aduOffset = self.sam.calibrateRefOffsetAndGain()

        return aduPerVolt, aduOffset
