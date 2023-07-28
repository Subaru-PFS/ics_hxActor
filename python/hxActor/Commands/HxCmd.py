#!/usr/bin/env python

from importlib import reload

import logging
import os.path
import pickle
import threading
import time

import numpy as np

import astropy.time
import astropy.io.fits as pyfits

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

from ics.utils.fits import mhs as fitsUtils
from ics.utils.fits import fitsWriter
from ics.utils.fits import timecards as actortime
from ics.utils.fits import wcs
from ics.utils.sps import fits as spsFits
from ics.utils import time as pfsTime

from ics.utils.sps import hxramp
from hxActor.Commands import ramp
from hxActor.Commands import rampSim

reload(fitsWriter)
reload(hxramp)
reload(ramp)
reload(rampSim)
reload(spsFits)

def isoTs(t=None):
    if t is None:
        ts = pfsTime.Time.now()
    else:
        ts = pfsTime.Time.fromtimestamp(t)

    return ts.isoformat()

class HxCmd(object):

    def __init__(self, actor):

        # This lets us access the rest of the actor.
        self.actor = actor
        self.logger = self.actor.logger

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('hx', '@raw', self.hxRaw),
            ('bounce', '', self.bounce),
            ('reconnect', '[<firmwareFile>] [<configName>] [@bouncePower]', self.reconnect),
            ('hxconfig',
             '[<configName>] [<interleaveRatio>] [<interleaveOffset>] [<preampGain>] [<numOutputs>] '
             '[<idleModeOption>]',
             self.hxconfig),
            ('reconfigAsic', '', self.reconfigAsic),
            ('getVoltage', '<name>', self.sampleVoltage),
            ('getVoltageSettings', '', self.getVoltageSettings),
            ('getVoltages', '', self.getVoltages),
            ('getSpiRegisters', '', self.getSpiRegisters),
            ('getRefCal', '', self.getRefCal),
            ('getTelemetry', '', self.getTelemetry),
            ('getAsicPower', '', self.getAsicPower),
            ('getAsicErrors', '', self.getAsicErrors),
            ('idleAsic', '', self.idleAsic),
            ('resetAsic', '', self.resetAsic),
            ('powerOffAsic', '', self.powerOffAsic),
            ('powerOnAsic', '', self.powerOnAsic),
            ('setVoltage', '<name> <voltage>', self.setVoltage),
            ('ramp',
             '[<nramp>] [<nreset>] [<nread>] [<ngroup>] [<ndrop>] [<itime>] '
             '[<visit>] [<exptype>] [<objname>] [<expectedExptime>] '
             '[<lamp>] [<lampPower>] [<readoutSize>] [@noOutputReset] [@rawImage]',
             self.takeOrSimRamp),
            ('ramp', 'finish [<exptime>] [<obstime>] [@stopRamp]', self.finishRamp),
            ('reloadLogic', '', self.reloadLogic),
            ('readAsic', '<reg> [<nreg>]', self.getAsicReg),
            ('writeAsic', '<reg> <value>', self.writeAsicReg),
            ('readSam', '<reg> [<nreg>]', self.getSamReg),
            ('setReadSpeed', '@(fast|slow) [@debug]', self.setReadSpeed),
            ('grabAllH4Info', '[@doRef]', self.grabAllH4Info),
            ('clearRowSkipping', '', self.clearRowSkipping),
            ('setRowSkipping', '<skipSequence>', self.setRowSkipping),
            ('downloadMcdFile', '<firmwareFile>', self.downloadMcdFile),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("hx", (1, 2),
                                        keys.Key("visit", types.Int(), default=None,
                                                 help='If set, the assigned visit number.'),
                                        keys.Key("nramp", types.Int(), default=1,
                                                 help='number of ramps to take.'),
                                        keys.Key("nreset", types.Int(), default=1,
                                                 help='number of resets to make.'),
                                        keys.Key("nread", types.Int(), default=2,
                                                 help='number of reads to take.'),
                                        keys.Key("ngroup", types.Int(), default=1,
                                                 help='number of groups.'),
                                        keys.Key("ndrop", types.Int(), default=0,
                                                 help='number of drops to waste.'),
                                        keys.Key("itime", types.Float(), default=None,
                                                 help='desired integration time'),
                                        keys.Key("exptime", types.Float(), default=None,
                                                 help='real illumination time'),
                                        keys.Key("expectedExptime", types.Float(), default=None,
                                                 help='expected actual illumination time'),
                                        keys.Key("obstime", types.String(), default=None,
                                                 help='time at start of illumination.'),
                                        keys.Key("exptype", types.String(), default=None,
                                                 help='What to put in IMAGETYP/DATA-TYP.'),
                                        keys.Key("objname", types.String(), default=None,
                                                 help='What to put in OBJECT.'),
                                        keys.Key("configName", types.String(), default=None,
                                                 help='configuration name'),
                                        keys.Key("firmwareFile", types.String(), default=None,
                                                 help='name of ASIC firmware file or path'),
                                        keys.Key("name", types.String(), default=None,
                                                 help='voltage name'),
                                        keys.Key("voltage", types.Float(), default=None,
                                                 help='voltage'),
                                        keys.Key("lamp", types.Int(), default=0,
                                                 help='lamp name'),
                                        keys.Key("lampPower", types.Int(), default=0,
                                                 help='lamp power (0..1023)xs'),
                                        keys.Key("readoutSize", types.Int()*2, default=0,
                                                 help='actual cols,rows of readout'),
                                        keys.Key("reg", types.String(),
                                                 help='register number (hex or int)'),
                                        keys.Key("value", types.String(),
                                                 help='register value (hex or int)'),
                                        keys.Key("nreg", types.Int(), default=1,
                                                 help='number of registers to read'),
                                        keys.Key("interleaveOffset", types.Int(),
                                                 help="after how many pixels is the ref. pixel read"),
                                        keys.Key("interleaveRatio", types.Int(),
                                                 help="the ratio between science and reference pixels."),
                                        keys.Key("preampGain", types.Int(),
                                                 help="the index of the preamp gain setting."),
                                        keys.Key("numOutputs", types.Int(),
                                                 help="the number of channels to read H4 with. 1,4,16,32."),
                                        keys.Key("idleModeOption", types.Int(),
                                                 help="what to do while idle: 0=nothing, 1=reset, 2=reset+read"),
                                        keys.Key('skipSequence', types.Int()*5,
                                                 help="read/skip/read/skip/total sequence for rowSkipping")
                                        )

        self.backend = 'hxhal'
        self.rampConfig = None
        self.skipSequence = [0, 0, 0, 0, 4096]
        self.rampRunning = False

        if self.actor.instrument == "CHARIS":
            self.dataRoot = "/home/data/charis"
            self.dataPrefix = "CRSA"
            filenameFunc = None
        else:
            try:
                site = self.actor.ids.site
                dataRoot = self.actor.actorConfig['dataRoot']
                rampRoot = self.actor.actorConfig[site].get('rampRoot', None)
                doCompress = self.actor.actorConfig[site].get('compress', True)
            except Exception as e:
                raise RuntimeError(f'failed to fetch dataRoot, etc. for {site}: {e}')

            self.dataRoot = dataRoot
            self.dataPrefix = None
            self.logger.info(f'using dataRoot={self.dataRoot} with rampRoot={rampRoot} site={site}')

            # We want the fits writing process to be persistent, mostly so that
            # we do not have to pay attention to when it finishes.
            self.rampBuffer = fitsWriter.FitsBuffer(doCompress=doCompress, rampRoot=rampRoot)

            import pfs.utils.butler as pfsButler
            reload(pfsButler)
            butler = pfsButler.Butler(specIds=self.actor.ids)

            def filenameFunc(dataRoot, visit, butler=butler, logger=self.logger):
                """ Return the ramp filename """

                fileNameB = butler.getPath('rampFile', visit=visit)
                logger.info(f'butler returned {fileNameB}')
                return None, fileNameB

        from hxActor.charis import seqPath
        reload(seqPath)
        self.fileGenerator = seqPath.NightFilenameGen(self.dataRoot,
                                                      namesFunc = filenameFunc,
                                                      filePrefix=self.dataPrefix)
        self.everRun = False

    @property
    def controller(self):
        return self.actor.controllers.get(self.backend, None)

    @property
    def sam(self):
        ctrlr = self.actor.controllers.get(self.backend, None)
        return ctrlr.sam

    def bounce(self, cmd):
        self.controller.disconnect()

    def reconnect(self, cmd, doFinish=True):
        """Reconnect to SAM and ASIC. Optionally power-cycle and/or reload ASIC firmware and reconfigure.

        If 'bouncePower' is True, then always load firmware, etc.
        """

        cmdKeys = cmd.cmd.keywords
        firmwareFile = cmdKeys['firmwareFile'].values[0] if 'firmwareFile' in cmdKeys else None
        configName = cmdKeys['configName'].values[0] if 'configName' in cmdKeys else None
        bouncePower = 'bouncePower' in cmdKeys

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        self.controller.reconnect(bouncePower=bouncePower,
                                  firmwareName=firmwareFile, configName=configName, cmd=cmd)
        self.getHxConfig(cmd=cmd, doFinish=doFinish)

    def hxconfig(self, cmd, doFinish=True):
        """Set the given hxhal configuration. """

        cmdKeys = cmd.cmd.keywords
        tweaks = dict()
        if 'numOutputs' in cmdKeys:
            numChannel = cmdKeys['numOutputs'].values[0]
            if numChannel not in {0,1,4,16,32}:
                cmd.fail(f'text="invalid numOutputs={numChannel}. Must be 0,1,4,16,32"')
                return
            tweaks['numOutputs'] = numChannel
        if 'preampGain' in cmdKeys:
            preampGain = cmdKeys['preampGain'].values[0]
            if preampGain < 0 or preampGain > 15:
                cmd.fail('text="invalid gain setting={preampGain}. Must be 0..15"')
            tweaks['preampGain'] = preampGain

        if 'interleaveRatio' in cmdKeys:
            interleaveRatio = cmdKeys['interleaveRatio'].values[0]
            if interleaveRatio < 0 or interleaveRatio > 8:
                cmd.fail('text="invalid interleave ratio={interleaveRatio}. Must be 0..8"')
            tweaks['interleaveRatio'] = interleaveRatio

        if 'interleaveOffset' in cmdKeys:
            interleaveOffset = cmdKeys['interleaveOffset'].values[0]
            if interleaveOffset < 0 or interleaveOffset > 8:
                cmd.fail('text="invalid interleave ratio={interleaveOffset}. Must be 0..8"')
            tweaks['interleaveOffset'] = interleaveOffset

        if 'idleModeOption' in cmdKeys:
            idleModeOption = cmdKeys['idleModeOption'].values[0]
            if idleModeOption < 0 or idleModeOption > 2:
                cmd.fail('text="invalid idle mode action={idleModeOption}. Must be 0..2"')
            tweaks['idleModeOption'] = idleModeOption

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        configName = cmdKeys['configName'].values[0]

        sam = self.sam

        try:
            configGroup, configName = configName.split('.')
        except:
            configGroup = 'h4rgConfig' if self.actor.instrument == 'PFS' else 'h2rgConfig'

        sam.updateHxRgConfigParameters(configGroup, configName, tweaks=tweaks)
        self.clearRowSkipping(cmd, doFinish=False)
        self.getHxConfig(cmd=cmd, doFinish=False)

        if doFinish:
            cmd.finish()

    def downloadMcdFile(self, cmd):
        """Download a named .mcd file."""

        firmwareFile = cmd.cmd.keywords['firmwareFile'].values[0]

        cmd.inform(f'text="downloading .mcd file: {firmwareFile}"')
        self.sam.downloadMcdFile(firmwareFile)
        cmd.finish(f'text="download done"')

    def getRowSequence(self, cmd):
        read1 = self.sam.link.ReadAsicReg(0x4300)
        skip1 = self.sam.link.ReadAsicReg(0x4301)
        read2 = self.sam.link.ReadAsicReg(0x4302)
        skip2 = self.sam.link.ReadAsicReg(0x4303)
        total = self.sam.link.ReadAsicReg(0x4034)

        asicSequence = [read1, skip1, read2, skip2, total]
        if self.skipSequence != asicSequence:
            cmd.warn(f'text="skipSequence ({asicSequence}) did not match expected ({self.skipSequence})"')
            return self.skipSequence

        return asicSequence

    def reportRowSequence(self, cmd, doFinish=False):
        read1,skip1,read2,skip2,total = seq = self.getRowSequence(cmd)
        msg = f'skipSequence={total != 4096},{read1},{skip1},{read2},{skip2},{total}'
        if doFinish:
            cmd.finish(msg)
        else:
            cmd.inform(msg)

        return seq

    def setRowSkipping(self, cmd):
        read1, skip1, read2, skip2, total = cmd.cmd.keywords['skipSequence'].values
        self.skipSequence = [read1, skip1, read2, skip2, total]

        self.sam.link.WriteAsicReg(0x4300, read1)
        self.sam.link.WriteAsicReg(0x4301, skip1)
        self.sam.link.WriteAsicReg(0x4302, read2)
        self.sam.link.WriteAsicReg(0x4303, skip2)
        self.sam.link.WriteAsicReg(0x4034, total)

        # there is a per-frame size override. Clear that and recalculate.
        self.sam.overrideFrameSize(None)
        self.reportRowSequence(cmd, doFinish=True)

    def clearRowSkipping(self, cmd, doFinish=True):
        self.skipSequence = [0, 0, 0, 0, 4096]
        self.sam.link.WriteAsicReg(0x4034, 4096)
        self.sam.link.WriteAsicReg(0x4300, 0)
        self.sam.link.WriteAsicReg(0x4301, 0)
        self.sam.link.WriteAsicReg(0x4302, 0)
        self.sam.link.WriteAsicReg(0x4303, 0)
        self.sam.overrideFrameSize(None)

        self.reportRowSequence(cmd, doFinish=doFinish)

    def reconfigAsic(self, cmd):
        """Trigger the ASIC reconfig process. """

        self.sam.reconfigureAsic()
        self.getHxConfig(cmd, doFinish=False)
        cmd.finish()

    def updateDaqState(self, cmd, always=False):
        """Make sure our snapshot of the ASIC config is valid. """

        if always or not self.controller.daqState.isValid:
            self.getHxConfig(cmd, doFinish=False)

    def getHxConfig(self, cmd, doFinish=True):
        self.grabAllH4Info(cmd, doFinish=False)
        cfg = self.controller.daqState.hxConfig
        self.reportRowSequence(cmd)

        keys = []
        if not cfg.h4Interleaving:
            keys.append('irp=False,0,0')
        else:
            keys.append(f'irp={cfg.h4Interleaving},{cfg.interleaveRatio},{cfg.interleaveOffset}')
        gainFactor = self.sam.getGainFromTable(cfg.preampGain)
        keys.append(f'preamp={cfg.preampGain},{gainFactor},{cfg.preampInputScheme},{cfg.preampInput},'
                    f'{cfg.preampInput1ByUser},{cfg.preampInput8ByUser}')
        keys.append(f'window={cfg.bWindowMode},{cfg.xStart},{cfg.xStop},{cfg.yStart},{cfg.yStop}')

        readTime = self.calcFrameTime()
        keys.append(f'readTime={readTime:0.4f}')

        try:
            keys.append(f'asicVersion={cfg.version}')
        except KeyError:
            keys.append(f'asicVersion=UNKNOWN')

        for k in keys:
            cmd.inform(k)

        if doFinish:
            cmd.finish()

        return cfg

    def setVoltage(self, cmd):
        """Set a single Hx bias voltage. """

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        voltageName = cmdKeys['name'].values[0]
        voltage = cmdKeys['voltage'].values[0]

        sam = self.sam

        try:
            newVoltage = sam.setBiasVoltage(voltageName, voltage)
        except Exception as e:
            cmd.fail('text="Failed to set voltage %s=%s: %s"' % (voltageName,
                                                                 voltage,
                                                                 e))
        self.sampleVoltage(cmd=cmd, doFinish=False)
        cmd.finish(f'text="set {voltageName} to {newVoltage:.3f}"')

    def sampleVoltage(self, cmd, doFinish=True):
        """Sample a single Hx bias voltage. """

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        voltageName = str(cmdKeys['name'].values[0])
        setting, reading, raw = self.controller.sampleVoltage(voltageName)

        cmdFunc = cmd.finish if doFinish else cmd.inform
        cmdFunc(f'text="{voltageName:12s} = {reading: .3f} set {setting: .3f}, raw {raw:#04x}"')

    def getVoltages(self, cmd):
        self.getRefCal(cmd, doFinish=False)
        for voltageName in self.sam.voltageNames:
            setting, reading, raw = self.controller.sampleVoltage(voltageName)
            cmd.inform(f'text="{voltageName:12s} = {reading: .3f} set {setting: .3f}, raw {raw:#04x}"')

        cmd.finish()

    def getVoltageSettings(self, cmd, doFinish=True):
        """Query for and report all bias voltage settings. """

        settings = self.controller.getVoltageSettings()

        for name, setting in settings.items():
            cmd.inform(f'text="{name:12s} = {setting: .3f}"')
        if doFinish:
            cmd.finish()

    def getMainVoltages(self, cmd, doFinish=False):
        cmdKeys = cmd.cmd.keywords
        doRef = 'doRef' in cmdKeys

        ret = self.controller.getMainVoltages(doRef=doRef)
        cmdFunc = cmd.finish if doFinish else cmd.inform
        for voltageName in ret.keys():
            setting, reading, raw = ret[voltageName]
            cmdFunc(f'text="{voltageName:12s} = {reading: .3f} set {setting: .3f}, raw {raw:#04x}"')

        if doFinish:
            cmd.finish()

    def getRefCal(self, cmd, doFinish=True):
        """Sample the ASIC refence offset and gain. """

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        aduPerVolt, aduOffset = self.sam.calibrateRefOffsetAndGain()

        cmdFunc = cmd.finish if doFinish else cmd.inform
        cmdFunc(f'text=" offset={aduOffset:#04x}/{aduOffset}; '
                f'ADU/V={aduPerVolt} uV/ADU={1e6/aduPerVolt:0.1f}"')

    def getAsicReg(self, cmd):
        """Read ASIC register(s). """

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        regnum = cmdKeys['reg'].values[0]
        nreg = cmdKeys['nreg'].values[0] if 'nreg' in cmdKeys else 1

        sam = self.sam

        try:
            regnum = int(regnum, base=16)
        except ValueError:
            cmd.fail(f'text="regnum ({regnum}) is not a valid hex number"')
            return

        for i in range(nreg):
            reg = regnum + i
            val = sam.link.ReadAsicReg(reg)
            cmd.inform('text="0x%04x = 0x%04x"' % (reg, val))

        cmd.finish()

    def writeAsicReg(self, cmd):
        """Write single ASIC register. """

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        regnum = cmdKeys['reg'].values[0]
        value = cmdKeys['value'].values[0]

        sam = self.sam

        try:
            regnum = int(regnum, base=16)
        except ValueError:
            cmd.fail(f'text="regnum ({regnum}) is not a valid hex number"')
            return

        try:
            value = int(value, base=0)
        except ValueError:
            cmd.fail(f'text="regnum ({regnum}) is not a valid ing or hex number"')
            return

        cmd.inform('text="setting 0x%04x = 0x%04x"' % (regnum, value))
        sam.link.WriteAsicReg(regnum, value)
        val = sam.link.ReadAsicReg(regnum)
        cmd.inform('text="0x%04x = 0x%04x"' % (regnum, val))

        cmd.finish()

    def getSamReg(self, cmd):
        """Read SAM/Jade register(s). """

        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        regnum = cmdKeys['reg'].values[0]
        nreg = cmdKeys['nreg'].values[0] if 'nreg' in cmdKeys else 1

        sam = self.sam

        try:
            regnum = int(regnum, base=16)
        except ValueError:
            cmd.fail(f'text="regnum ({regnum}) is not a valid hex number"')
            return

        for i in range(nreg):
            reg = regnum + i
            val = sam.link.ReadJadeReg(reg)
            cmd.inform('text="0x%04x = 0x%04x"' % (reg, val))

        cmd.finish()

    def hxRaw(self, cmd):
        """ Tunnel a rawCmd command to the HX program. """

        cmdKeys = cmd.cmd.keywords
        ctrl = self.controller

        rawCmd = cmdKeys['raw'].values[0]
        cmd.fail('text="not implemented"')

    def _calcAcquireTimeout(self, expType='ramp', cmd=None):
        """ Return the best estimate of the actual expected time for our current rampConfig. """

        frameTime = self.calcFrameTime()
        if expType == 'ramp':
            return frameTime * (self.rampConfig['nread'] +
                                self.rampConfig['nreset'] +
                                self.rampConfig['ndrop'])
        elif expType == 'single':
            return frameTime * (1 + self.rampConfig['nreset'])
        elif expType == 'CDS':
            return frameTime * (2 + self.rampConfig['nreset'])
        else:
            raise RuntimeError("unknown expType %s" % (expType))

    def flushProgramInput(self, cmd, doFinish=True):
        debris = ''
        while True:
            try:
                ret = self.controller.getOneChar(timeout=0.2)
                debris = debris + ret
            except RuntimeError:
                break
            except:
                raise

        if debris != '':
            cmd.warn('text="flushed stray input: %r"' % (debris))

        if doFinish:
            cmd.finish()

    def getSubaruHeader(self, frameId, timeout=1.0,
                        fullHeader=True, exptype='TEST', cmd=None):

        itime = self.calcFrameTime()
        if exptype.lower() == 'nohdr':
            return pyfits.Header()

        headerTask = subaru.FetchHeader(fullHeader=True, frameId=frameId, itime=itime, exptype=exptype)
        self.logger.debug('text="starting header task timeout=%s frameId=%s"' % (timeout, frameId))
        headerTask.start()
        headerQ = headerTask.q
        self.logger.info('text="header q: %s"' % (headerQ))

        try:
            hdrString = headerQ.get(True, timeout)
            if hdrString is None:
                self.logger.debug('text=".get header: %s"' % (hdrString))
            else:
                self.logger.debug('text=".get header: %s"' % (len(hdrString)))
        except Exception as e:
            self.logger.warn('text="failed to .getHeader header: %s"' % (e))
            cmd.warn('text="failed to .getHeader header: %s"' % (e))
            hdrString = ''
        finally:
            headerTask.terminate()
            time.sleep(0.1)

        hdr = pyfits.Header.fromstring(hdrString)

        return hdr

    def getHeader(self, frameId, fullHeader=True,
                  exptype='TEST',
                  timeout=1.0, cmd=None):
        try:
            hdr = self.getSubaruHeader(frameId, fullHeader=fullHeader,
                                       exptype=exptype,
                                       timeout=timeout, cmd=cmd)
        except Exception as e:
            self.logger.warn('text="failed to fetch Subaru header: %s"' % (e))
            cmd.warn('text="failed to fetch Subaru header: %s"' % (e))
            hdr = pyfits.Header()

        hxCards = self.getHxCards(cmd)
        for c in hxCards:
            hdr.append(c)

        scexaoCards = self.getSCExAOCards(cmd)
        for c in scexaoCards:
            hdr.append(c)

        charisCards = self.getCharisCards(cmd)
        for c in charisCards:
            hdr.append(c)

        return hdr

    def getHxCards(self, cmd=None):
        # voltageList = self.controller.getAllBiasVoltages
        return []

    def getSpiRegisters(self, cmd):
        h4Regs = self.sam.readAllH4SpiRegs()
        allBad = True
        for i, reg in enumerate(h4Regs):
            cmd.inform(f'spiReg%d=0x%04x' % (i, reg))
            if reg != 0:
                allBad = False
        if allBad:
            cmd.warn('h4SpiState="WARNING: H4 SPI registers are all 0! ASIC cannot read H4!"')
        else:
            cmd.inform('h4SpiState="OK"')
        cmd.finish()

    def idleAsic(self, cmd):
        self.sam.idleAsic()
        self.getAsicErrors(cmd)

    def resetAsic(self, cmd):
        self.sam.resetAsic()
        self.getAsicErrors(cmd)

    def powerOffAsic(self, cmd):
        self.sam.powerDownAsic()
        self.getAsicErrors(cmd)

    def powerOnAsic(self, cmd):
        self.sam.initAsics()
        self.getAsicErrors(cmd)

    def writeSpiRegister(self, cmd):
        pass

    def getAsicErrors(self, cmd):
        errorMask = self.sam.getAsicErrors()
        cmd.inform(f'asicErrors=0x%08x' % (errorMask))
        cmd.finish()

    def getTelemetry(self, cmd):
        volts, amps, labels = self.sam.telemetry()
        cmd.finish('text="see log for telemetry"')

    def getAsicPower(self, cmd):
        V,A,W = self.sam.printAsicPower()

        colLabels = ['Measurement', 'Voltage(V)', 'Current(mA)', 'Power(mW)']
        rowLabels = ['VDDA', 'Vref', 'VDD3p3', 'VDD', 'VDDIO']

        nBanks = 1
        nChan = 5
        for bank_i in range(nBanks):
            if bank_i > 0:
                cmd.inform('text=""')
            bankPower = 0.0
            for row_i in range(nChan):
                meas_i = bank_i + row_i*nBanks
                cmd.inform('text="bank %d  %-6s %+4.2fV %+5.3fA %0.3fW"' % (bank_i, rowLabels[row_i],
                                                                            V[meas_i], A[meas_i]/1000.0,
                                                                            W[meas_i]/1000.0))
                bankPower += W[meas_i]
            cmd.inform('text="bank %d total: %0.3fW"' % (bank_i, bankPower/1000.0))

        cmd.finish()

    def lamp(self, lamp, lampPower, cmd):
        if self.actor.ids.camName != 'n8':
            return

        from hxActor.Commands import opticslab
        reload(opticslab)

        if lamp != 0:
            opticslab.lampCmd(lamp, lampPower)
        cmd.inform('text="lamp %s=%s"' % (lamp, lampPower))

    def setHxCards(self, ramp, group, read, doClear=True):
        if doClear:
            self.hxCards = []

        self.hxCards.append(dict(name='W_H4RAMP', value=ramp, comment='the current ramp number'))
        self.hxCards.append(dict(name='W_H4GRUP', value=group, comment='the current group number'))
        self.hxCards.append(dict(name='W_H4READ', value=read, comment='the current read number'))

    def startLampCards(self, lamp, lampPower):
        self.lampCards = []
        if self.actor.ids.camName != 'n8':
            return

        self.lampCards.append(dict(name='W_OLLAMP', value=lamp, comment='Optics lab lamp'))
        self.lampCards.append(dict(name='W_OLLPLV', value=lampPower, comment='Optics lab lamp command level'))

    def getLastLampState(self, lamp, lampPower, cmd):
        self.lampCards = []
        if self.actor.ids.camName != 'n8':
            return

        from hxActor.Commands import opticslab
        reload(opticslab)

        try:
            current, lam, flux = opticslab.getFlux(lamp)
        except Exception:
            cmd.warn('text="failed to get lamp info for optics lab"')
            return

        cmd.inform('text="lamp %s=%s %s %s %s"' % (lamp, lampPower,
                                                   current, lam, flux))
        self.hxCards.append(dict(name='W_OLLAMP', value=lamp, comment='Optics lab lamp'))
        self.hxCards.append(dict(name='W_OLLPLV', value=lampPower, comment='Optics lab lamp command level'))
        self.hxCards.append(dict(name='W_OLLPWV', value=lam, comment='[nm] Lamp center wavelength'))
        self.hxCards.append(dict(name='W_OLLPCR', value=current, comment='[A] Photodiode current'))
        self.hxCards.append(dict(name='W_OLFLUX', value=flux, comment='[photons/s] Calibrated flux'))

    def placeSkippedRows(self, cmd, image, rowSequence):
        """Place the packed rows from a row-skipping read into a full-sized image.

        Slightly odd logic:
        - we get two pairs of (read, skip) regions, and a total number of rows.
        - read/place the first pair
        - keep placing the second read and skipping until the total has been hit.
        """
        read1, skip1, read2, skip2, total = rowSequence
        frameSize, _ = self.sam.calcFrameSize()
        cfg = self.sam.hxrgDetectorConfig
        height = 1024 * cfg.muxType

        newImage = np.zeros(shape=(height, frameSize[0]), dtype=image.dtype)
        haveRead = 0
        if read1 > 0:
            wantToRead = read1
            canRead = min(wantToRead, total)
            newImage[:canRead, :] = image[:canRead]
            haveRead += canRead
        nextStart = read1 + skip1
        cmd.inform(f'text="rows1={read1},{skip1},{total}; haveRead={haveRead},{nextStart} dest={newImage.shape}"')
        while haveRead < total:
            wantToRead = read2
            canRead = min(wantToRead, total-haveRead)
            cmd.inform(f'text="dest={nextStart},{wantToRead},{canRead}; src={haveRead},{wantToRead},{canRead}"')

            newImage[nextStart:nextStart+canRead, :] = image[haveRead:haveRead+canRead]
            haveRead += canRead
            nextStart += canRead
            nextStart += skip2
            cmd.inform(f'text="end2={haveRead},{total} nextStart={nextStart}"')

        cmd.inform(f'text="haveRead={total},{haveRead}"')
        return newImage

    def writeSingleRead(self, cmd, image, hdr, ramp, group, read, nChannel, irpOffset,
                        rawImage=False, rowSequence=None, isResetRead=False):
        """Write the image for a single read to disk.

        - splits out the DATA and IRP components
        - interpolates row-skipped images into full images
        - knows about reset frames.
        """

        if rawImage:
            data = image
            ref = None
        else:
            if rowSequence is not None:
                image = self.placeSkippedRows(cmd, image, rowSequence)
            data, ref = hxramp.splitIRP(image, nChannel=nChannel, refPix=irpOffset)

            # We always want the file to have IMAGE and REF HDUs:
            if ref is None:
                ref = data*0

        extnamePrefix = 'RESET_' if isResetRead else ''
        cmd.inform(f'text="adding HDUs at group={group} read={read} isReset={isResetRead} shape={data.shape} ref={data.shape} med={np.median(data)}"')
        self.rampBuffer.addHdu(data, hdr, hduId=(ramp, group, read),
                                extname=f'{extnamePrefix}IMAGE_{read}')
        if ref is not None:
            self.rampBuffer.addHdu(ref, None, hduId=(ramp, group, None),
                                    extname=f'{extnamePrefix}REF_{read}')

    def takeOrSimRamp(self, cmd):
        """Take a ramp, either from real DAQ/detector or from the simulator. """

        if self.actor.simulateOnly:
            self.simulateRamp(cmd)
        else:
            self.takeRamp(cmd)

    def simulateRamp(self, cmd):
        cmdKeys = cmd.cmd.keywords

        nreset = cmdKeys['nreset'].values[0] if ('nreset' in cmdKeys) else 1
        nread = cmdKeys['nread'].values[0] if ('nread' in cmdKeys) else 2
        ndrop = cmdKeys['ndrop'].values[0] if ('ndrop' in cmdKeys) else 0
        ngroup = cmdKeys['ngroup'].values[0] if ('ngroup' in cmdKeys) else 1
        visit = int(cmdKeys['visit'].values[0]) if ('visit' in cmdKeys) else 9999

        if nread < 1 or ngroup != 1 or ndrop != 0 or nreset not in {0,1} or 'noOutputReset' in cmdKeys:
            cmd.fail('text="will only simulate simple ramps (ngroup=1, nreset<=1, ndrop=0, nread>0"')
            return
        if 'readoutSize' in cmdKeys:
            cmd.fail('text="cannot simulate hacked readoutSize"')
            return

        rampSim.rampSim(cmd, visit, nread,
                        ngroup=ngroup,
                        nreset=nreset, ndrop=0,
                        readTime=10.857)

    def takeRamp(self, cmd):
        """Main exposure entry point.
        """

        cmdKeys = cmd.cmd.keywords

        nramp = cmdKeys['nramp'].values[0] if ('nramp' in cmdKeys) else 1
        nreset = cmdKeys['nreset'].values[0] if ('nreset' in cmdKeys) else 1
        nread = cmdKeys['nread'].values[0] if ('nread' in cmdKeys) else 2
        ndrop = cmdKeys['ndrop'].values[0] if ('ndrop' in cmdKeys) else 0
        ngroup = cmdKeys['ngroup'].values[0] if ('ngroup' in cmdKeys) else 1
        itime = cmdKeys['itime'].values[0] if ('itime' in cmdKeys) else None
        visit = cmdKeys['visit'].values[0] if ('visit' in cmdKeys) else 0
        exptype = cmdKeys['exptype'].values[0] if ('exptype' in cmdKeys) else 'TEST'
        objname = str(cmdKeys['objname'].values[0]) if ('objname' in cmdKeys) else None
        lamp = cmdKeys['lamp'].values[0] if ('lamp' in cmdKeys) else 0
        lampPower = cmdKeys['lampPower'].values[0] if ('lampPower' in cmdKeys) else 0
        readoutSize = cmdKeys['readoutSize'].values if ('readoutSize' in cmdKeys) else None
        idleModeOption = cmdKeys['idleModeOption'].values[0] if ('idleModeOption' in cmdKeys) else None
        outputReset = 'noOutputReset' not in cmdKeys
        rawImage = 'rawImage' in cmdKeys

        if idleModeOption is not None and (idleModeOption < 0 or idleModeOption > 2):
            cmd.fail('text="idleModeOption must be 0..2"')
            return

        if lamp > 0 and not outputReset:
            cmd.fail('text="can only turn on lamps when saving reset reads."')
            return

        if readoutSize is not None:
            cmd.warn(f'text="overriding readout size to cols={readoutSize[0]}, rows={readoutSize[1]}"')
            rowSequence = None
        else:
            rowSequence = self.reportRowSequence(cmd)
            nominalSize, _ = self.sam.calcFrameSize()

            if nominalSize[1] != rowSequence[-1]:
                readoutSize = [nominalSize[0], rowSequence[-1]]
                cmd.warn(f'text="rowSequence override: {rowSequence} to framesize {readoutSize}"')
            else:
                cmd.warn(f'text="rowSequence NO override: {rowSequence} vs {nominalSize}"')
                rowSequence = None

        self.lamp(lamp, 0, cmd)

        if self.rampRunning:
            cmd.fail('text="a ramp is already running!"')
            return

        cmd.diag('text="ramps=%s resets=%s reads=%s rdrops=%s rgroups=%s itime=%s visit=%s exptype=%s"' %
                 (nramp, nreset, nread, ndrop, ngroup, itime, visit, exptype))

        if itime is not None:
            if 'nread' in cmdKeys:
                cmd.fail('text="cannot specify both nread= and itime="')
                return
            nread = int(itime / self.sam.frameTime) + 1

        if nread <= 0 or nramp <= 0 or ngroup <= 0:
            cmd.fail('text="all of nramp,ngroup,(nread or itime) must be positive"')
            return

        cmd.inform('text="configuring ramp..."')
        self.nread = nread

        if not self.everRun:
            cmd.inform('text="blowing astropy nose..."')
            self.getTimeCards(cmd=cmd)
            self.everRun = True

        cmd.inform('ramp=%d,%d,%d,%d,%d' % (nramp,ngroup,nreset,nread,ndrop))
        cmd.inform('rampConfig=%d,%d,%d,%d,%d' % (visit,ngroup,nreset,nread,ndrop))

        self.updateDaqState(cmd, always=False)
        self.hxCards = []

        if self.backend == 'hxhal':
            t0 = time.time()
            sam = self.sam
            if sam is None:
                cmd.fail('text="the hxhal controller is not connected"')
                return

            if self.actor.instrument == 'PFS':
                runThreaded = True
                self.doStopRamp = False
                self.rampPatched = False
                if nramp != 1:
                    raise ValueError("PFS can only take one ramp at a time")

                seqno = None if visit == 0 else visit
                _, rampFilename = self.fileGenerator.getNextFileset(seqno=seqno)

                rampReporter = ramp.Ramp(cmd)
                # self.grabAllH4Info(cmd, doFinish=False)
                self.startLampCards(lamp, lampPower)
                self.setHxCards(0, 0, 0, doClear=True)

                headerCB =  None
                noFiles = True
                hxConfig = self.sam.hxrgDetectorConfig
                nChannel = hxConfig.numOutputs
                if hxConfig.h4Interleaving:
                    irpOffset = hxConfig.interleaveOffset
                else:
                    irpOffset = 0

                # INSTRM-1993 investigations: turn logging up before starting ramp.
                sam.link.readLogger.setLevel(logging.DEBUG)

                def readCB(ramp, group, read, filename, image,
                           lamp=lamp, lampPower=lampPower,
                           rowSequence=rowSequence):
                    """This is called by the DAQ routines at the end of each frame.

                    Returns:
                    --------
                    continueRamp : `bool`
                       whether the ramp should be allowed to continue.

                    """

                    global t0
                    self.setHxCards(ramp, group, read)

                    cmd.debug(f'text="cb ramp={ramp} group={group} read={read} '
                              f'image={image is not None} outputReset={outputReset}"')

                    # This call is made at the start of the RESET frame, and we use that to reference
                    # and advertise our frame timing. In this case, there is no image data.
                    if ramp == 0 and group == 0 and read == 0:
                        cmd.debug('text="DAQ output start"')
                        t0 = time.time()
                        self.readTime = self.calcFrameTime()
                        self.read0Start = t0 + nreset*self.readTime
                        resetStartStamp = isoTs(t0)
                        self.read0StartStamp = isoTs(self.read0Start)
                        self.exptime = nread * self.readTime
                        cmd.inform(f'readTimes={visit},{resetStartStamp},{self.read0StartStamp},{self.readTime:0.3f}')
                        return True

                    # We are starting a ramp: either with a reset read to write or without.
                    # In either case, generate RESET HDU(s) if wanted.
                    if ((outputReset and group == 0 and read == 1)
                            or ((not outputReset or nreset == 0) and group == 1 and read == 1)):
                        cmd.inform(f'text="creating FITS files at group={group} read={read}"')
                        if lampPower != 0:
                            cmd.inform(f'text="turning on flat lamp {lamp}@{lampPower}"')
                            self.lamp(lamp, lampPower, cmd)

                        phdr = self.getPfsHeader(visit=visit, exptype=exptype,
                                                 obstime=self.read0StartStamp,
                                                 objname=objname, cmd=cmd)
                        self.logger.info(f'filename={rampFilename}')
                        self.rampBuffer.createFile(rampReporter, rampFilename, phdr)

                    if group == ngroup-1 and read == nread-1:
                        self.getLastLampState(lamp, lampPower, cmd)

                    if group == 0:  # Reset read
                        if outputReset:
                            resetImageToWrite = 0 if image is None else image
                            hdr = self.getResetHeader(cmd)
                            self.writeSingleRead(cmd, resetImageToWrite, hdr, ramp, group, read, nChannel,
                                                 irpOffset, rawImage=rawImage, rowSequence=rowSequence,
                                                 isResetRead=True)
                    else:       # Non reset read
                        hdr = self.getPfsHeader(visit=visit, exptype=exptype,
                                                objname=objname, fullHeader=False, cmd=cmd)
                        self.writeSingleRead(cmd, image, hdr, ramp, group, read, nChannel, irpOffset,
                                             rawImage=rawImage, rowSequence=rowSequence, isResetRead=False)
                        # INSTRM-1993 investigations: turn logging off after first read done.
                        sam.link.readLogger.setLevel(logging.INFO)
                    if self.doStopRamp:
                        cmd.warn(f'text="stopping ramp at read {read}..."')
                        self.nread = read
                        patchCards = [dict(name='W_H4NRED', value=read,
                                           comment='Stopped number of ramp reads')]
                        self.logger.info('amending PHDU nread...')
                        self.rampBuffer.amendPHDU(patchCards)
                        # sam.waitForAsicIdle()
                    if group >= ngroup and read == nread or self.doStopRamp:
                        cmd.diag(f'text="closing FITS file from read cb..."')
                        self._doFinishRamp(cmd)
                        self.rampBuffer.finishFile()
                        if lampPower != 0:
                            self.lamp(lamp, 0, cmd)
                        if self.doStopRamp:
                            cmd.diag('text="idling ASIC and clearing SAM FIFO"')
                            self.sam.idleAsic()
                        return self.doStopRamp is not True

                    return True
            else:
                runThreaded = False
                sam.fileGenerator = self.fileGenerator
                noFiles = False
                rampReporter = None

                def readCB(ramp, group, read, filename, image):
                    cmd.inform('hxread=%s,%d,%d' % (filename, group, read))
                    if read == nread-1:
                        self.getLastState(lamp, lampPower, cmd)
                    if read == nread:
                        cmd.inform('filename=%s' % (filename))
                    return True

                def headerCB(ramp, group, read, seqno):
                    hdr = self.getCharisHeader(seqno=seqno, fullHeader=(read == 1), cmd=cmd)
                    return hdr.cards

            rampArgs = (cmd, sam, nramp, nreset, nread, ndrop, visit,
                        exptype, outputReset, readoutSize,
                        noFiles, rampReporter, headerCB, readCB, t0)
            if runThreaded:
                cmd.debug(f'text="launching ramp thread, with {len(threading.enumerate())} active threads: {threading.enumerate()}"')
                rampThread = threading.Thread(target=self.runRamp, name=f'ramp_{visit}',
                                              args=rampArgs, daemon=True)
                cmd.inform('text="starting ramp thread"')
                rampThread.start()
            else:
                self.runRamp(*rampArgs)

        else:
            # Use the Windows/IDL Teledyne server.
            dosplit = 'splitRamps' in cmdKeys
            self.winRead(cmd, nramp, nreset, nread, ngroup, ndrop, dosplit)

    def runRamp(self, cmd, sam,
                nramp, nreset, nread, ndrop, visit,
                exptype, outputReset, readoutSize,
                noFiles, rampReporter, headerCB, readCB, t0):
        """Run and finish a fully prepared ramp.

        This method is intended to be callable as a Thread target.
        """

        self.rampRunning = True
        try:
            sam.takeRamp(nResets=nreset, nReads=nread, nRamps=nramp,
                         exptype=exptype,
                         outputReset=outputReset,
                         actualFrameSize=readoutSize,
                         readCallback=readCB)
        except Exception as e:
            cmd.fail(f'text="ramp failed! -- {e}"')
            return
        finally:
            cmd.diag(f'text="closing FITS file from read thread..."')
            self.rampRunning = False
            sam.overrideFrameSize(None)

        cmd.inform('text="acquisition done; waiting for files to be closed."')
        t1 = time.time()
        dt = t1-t0
        cmd.inform('text="%d ramps, elapsed=%0.3f, perRamp=%0.3f, perRead=%0.3f"' %
                   (nramp, dt, dt/nramp, dt/(nramp*(nread+nreset+ndrop))))
        # Now possibly wait on the fitsWriter processes.
        if rampReporter is not None:
            waitFor = 60
            waitUntil = time.time() + waitFor
            while True:
                if rampReporter.isFinished:
                    break
                now = time.time()
                if now > waitUntil:
                    cmd.warn(f'text="file writing process did not finish within {waitFor} seconds"')
                    break
                time.sleep(0.5)
        t1 = time.time()
        dt = t1-t0
        cmd.finish('text="%d ramps, elapsed=%0.3f, perRamp=%0.3f, perRead=%0.3f"' %
                   (nramp, dt, dt/nramp, dt/(nramp*(nread+nreset+ndrop))))

    def finishRamp(self, cmd):
        """Prepare to finish a ramp. Optionally stop a ramp at the end of this read."""

        cmdKeys = cmd.cmd.keywords

        exptime = float(cmdKeys['exptime'].values[0]) if ('exptime' in cmdKeys) else None
        obstime = str(cmdKeys['obstime'].values[0]) if ('obstime' in cmdKeys) else self.read0StartStamp
        doStopRamp = 'stopRamp' in cmdKeys

        if not self.rampRunning:
            cmd.fail('text="no active ramp to finish"')
            return

        if doStopRamp:
            self.doStopRamp = True

        self._doFinishRamp(cmd, obstime, exptime)
        cmd.finish(f'text="finishRamp: doStop={doStopRamp}"')

    def _doFinishRamp(self, cmd, obstime=None, exptime=None):
        """Patch the PHDU at the end of the ramp.

        In regular PFS operation, iic will call this just after the shutter is closed, and will
        provide obstime (exposure start) and exptime. This will let us update the following in
        the PHDU:
          - time cards
          - lamp cards
          - any _END cards
        If the ramp is stopped, also update the W_H4NRED card.

        WARNING: if a card is *added* to the PHDU, any later cards to patch will
                 not be patched but instead added in duplicate to the PHDU.
        """

        if self.rampPatched:
            return
        self.rampPatched = True

        patchCards = []

        if exptime is not None and obstime is not None:
            newTimeCards, exptime = self.getTimeCards(cmd, obstime=obstime, exptime=exptime)
            patchCards.extend(newTimeCards)

        newLampCards = self.hdrMgr.genLampCards(cmd, exptime)
        patchCards.extend(newLampCards)
        patchCards.append(dict(name='W_H4PTCH', value=True, comment='PHDU has been patched'))

        for c in patchCards:
            self.logger.info('  amending with %s:%s', type(c), c)
        self.logger.info('amending PHDU...')
        self.rampBuffer.amendPHDU(patchCards)

    def winRead(self, cmd, nramp, nreset, nread, ngroup, ndrop, dosplit):
        nrampCmds = nramp if dosplit else 1
        self.flushProgramInput(cmd, doFinish=False)

        ctrlr = self.controller
        ret = ctrlr.sendOneCommand('setRampParam(%d,%d,%d,%d,%d)' %
                                   (nreset,nread,ngroup,ndrop,(1 if dosplit else nramp)),
                                   cmd=cmd)
        if ret != '0:succeeded':
            cmd.fail('text="failed to configure for ramp: %s"' % (ret))
            return

        self.winGetconfig(cmd, doFinish=False)

        timeout = self._calcAcquireTimeout(expType='ramp')
        if not dosplit:
            timeout *= nramp
        timeout += 10

        t0 = time.time()
        for r_i in range(nrampCmds):
            cmd.inform('text="acquireramp command %d of %d"' % (r_i+1, nrampCmds))
            ctrlr.sendOneCommand('acquireramp',
                                 cmd=cmd,
                                 timeout=timeout,
                                 noResponse=True)
            self._consumeRamps((1 if dosplit else nramp),
                               ngroup,nreset,nread,ndrop,
                               cmd=cmd)
            ret = ctrlr.getOneResponse(cmd=cmd)
            if ret != '0:Ramp acquisition succeeded':
                cmd.fail('text="IDL gave unexpected response at end of ramp: %s"' % (ret))
                return

        t1 = time.time()
        dt = t1-t0
        cmd.finish('text="%d ramps, elapsed=%0.3f, perRamp=%0.3f, perRead=%0.3f"' %
                   (nramp, dt, dt/nramp, dt/(nramp*(nread+nreset+ndrop))))

    def _getMhsHeader(self, cmd):
        """ Gather FITS cards from all other actors we are interested in. """

        t0 = time.time()
        cmd.debug('text="fetching MHS cards..."')
        models = set(self.actor.models.keys())
        models = sorted(models - {self.actor.name})
        cards = fitsUtils.gatherHeaderCards(cmd, self.actor, modelNames=models, shortNames=True)
        cmd.debug('text="fetched %d MHS cards..."' % (len(cards)))
        t1 =  time.time()
        if t1 - t0 > 1:
            cmd.warn(f'text="it took {t1-t0:0.2f} seconds to fetch MHS cards!"')

        return cards

    def _getH4MhsHeader(self, cmd):
        """ Gather FITS cards from ourself. """

        cmd.debug(f'text="fetching {self.actor.name} MHS cards..."')
        models = {self.actor.name}
        cards = fitsUtils.gatherHeaderCards(cmd, self.actor, modelNames=models, shortNames=True)
        cmd.debug('text="fetched %d MHS cards..."' % (len(cards)))

        return cards

    def _getHxHeader(self, cmd):
        """ Gather FITS cards from ourselves. """

        cmd.debug('text="fetching HX cards..."')
        cards = self.hxCards
        cmd.debug('text="fetched %d HX cards..."' % (len(cards)))

        return cards

    def grabAllH4Info(self, cmd, doFinish=True):
        """Squirrel away all reasonably available ASIC/SAM/H4 info.

        It is OK to command the ASIC or the SAM, but not to take *too
        long*. This method is called before the ramp is started, but
        the caller might not expect a long or variable delay.

        In practice, this means that we current grab:
        - the ASIC configuration dictionary.
        - the H4 SPI registers
        - the ASIC voltage settings
        - _some_ ASCI voltage readings.

        """

        self.controller.grabAllH4Info()
        self.getVoltageSettings(cmd, doFinish=False)
        self.getMainVoltages(cmd, doFinish=False)
        self.controller.daqState.isValid =  True

        if doFinish:
            cmd.finish()

    def calcFrameTime(self):
        """Calculate the net time to readout a single read/frame.

        We cheat slightly, but should fix that. Basically, there are
        two ASIC registers which give the number of pixel times per
        row and the number of row times per read. The IRP firmware
        always pads the row times by 9 pixels, and I am pulling that
        out rather than calculating it on the fly. But we do believe
        the timings using that, for all number of channels and (I
        think) all IRP ratios.

        """

        cfg = self.controller.daqState.hxConfig

        frameSize, _ = self.sam.calcFrameSize()
        width, height = frameSize

        pixTime = cfg.pixelTime
        chanWidth = width//cfg.numOutputs

        # Get this correctly, with h4008 - 4096/cfg.numOutputs (137 - 128 for 32 channels)
        rowPad = 9

        # And h4009:
        framePad = 1

        frameTime = pixTime*(chanWidth + rowPad) * (height + framePad)
        self.logger.info(f'calcFrameTime: {frameTime} pixTime={pixTime} frameSize={frameSize} '
                         f'chanWidth={chanWidth}')
        return frameTime

    def genAllH4Cards(self, cmd):
        """Return the H4 cards for the PHDU. Consumes what .grabAllH4Info() gathered
        """

        voltageCardNames = dict(VReset='W_4VRST',
                                DSub='W_4DSUB',
                                VBiasGate='W_4VBG',
                                VBiasPower='W_4VBP',
                                CellDrain='W_4CDRN',
                                Drain='W_4DRN',
                                VDDA='W_4VDDA',
                                VDD='W_4VDD',
                                Vrefmain='W_4VRM')

        # *Start* with the MHS dictionary cards, then overwrite what we know better about.
        cards = self._getH4MhsHeader(cmd)

        daqState = self.controller.daqState

        for i, reg in enumerate(self.controller.daqState.spiRegisters):
            cards.append(dict(name=f'W_4SPI{i+1:02d}', value=reg, comment=f'H4 SPI register {i+1}'))
        for name, setting in daqState.voltageSettings.items():
            cardName = voltageCardNames.get(name)
            if cardName is None:
                continue
            cards.append(dict(name=f'{cardName}S', value=np.round(setting, 4), comment=f'[V] {name} setting'))

        for name, reading in daqState.voltageReadings.items():
            cardName = voltageCardNames.get(name)
            if cardName is None:
                continue
            cards.append(dict(name=f'{cardName}V', value=np.round(reading, 4), comment=f'[V] {name} reading'))

        def _replaceCard(cards, newCard):
            for c_i, c in enumerate(cards):
                if c['name'] == newCard['name']:
                    cards.pop(c_i)
                    break
            cards.append(newCard)

        def _replaceCardValue(cards, cardName, newValue):
            for c_i, c in enumerate(cards):
                if c['name'] == cardName:
                    c['value'] = newValue
                    break

        cfg = daqState.hxConfig
        frameTime = self.calcFrameTime()
        _replaceCard(cards, dict(name="W_FRMTIM", value=frameTime,
                                 comment='[s] individual read time, per ASIC'))
        _replaceCard(cards, dict(name="W_H4FRMT", value=frameTime,
                                 comment='[s] individual read time, per ASIC'))
        _replaceCard(cards, dict(name='W_H4IRP', value=int(cfg.h4Interleaving),
                                 comment='whether we are using IRP-enabled firmware'))
        _replaceCard(cards, dict(name='W_H4IRPN', value=int(cfg.interleaveRatio),
                                 comment='the number of data pixels per ref pixel'))
        _replaceCard(cards, dict(name='W_H4IRPO', value=int(cfg.interleaveOffset),
                                 comment='how many data pixels before the ref pixel'))

        _replaceCard(cards, dict(name='W_H4NCHN', value=int(cfg.numOutputs),
                                 comment='how many readout channels we have'))
        _replaceCard(cards, dict(name='W_H4GNST', value=int(cfg.preampGain),
                                 comment='the ASIC preamp gain setting'))
        _replaceCard(cards, dict(name='W_H4GAIN', value=self.sam.getGainFromTable(cfg.preampGain),
                                 comment='the ASIC preamp gain factor'))
        _replaceCard(cards, dict(name='W_H4GAIN', value=self.sam.getGainFromTable(cfg.preampGain),
                                 comment='the ASIC preamp gain factor'))

        try:
            ver = self.sam.instrumentTweaks.formatVersion
        except:
            cmd.warn('text="No defined H4 ramp format version, using 0"')
            ver = 0
        _replaceCard(cards, dict(name='W_4FMTVR', value=ver,
                                 comment='Data format version'))

        try:
            serials = self.actor.actorConfig['serialNumbers']
            _replaceCardValue(cards, 'W_SRH4', serials['h4'])
            _replaceCardValue(cards, 'W_SRASIC', serials['asic'])
            _replaceCardValue(cards, 'W_SRSAM', serials['sam'])
        except Exception as e:
            cmd.warn(f'text="failed to set H4 serial cards: {e}"')

        return cards

    def genJhuCards(self, cmd):
        allCards = []
        if len(self.lampCards) > 0:
            allCards.extend(self.lampCards)

        return allCards

    def getTimeCards(self, cmd, exptype='', obstime=None, exptime=None):
        """Get all Subaru-compliant FITS time cards.

        Args
        ----
        cmd : `Command`
          Command to report warnings back to
        exptype : `str`
          Exposure type -- if not `dark` we let sps tell us what the exposure time is.
        obstime : `str`
          The correct obstime, if we want to change it
        exptime : `float`
          The correct exposure time, if we want to change it

        Returns
        -------
        timecards : list of fitsio-compliant dicts
          all the time cards to insert in the header
        expTime : `float`
          our best guess of the actual illumination time.
        """

        cmdKeys = cmd.cmd.keywords

        t0 = time.time()
        if exptime is not None:
            exptime = float(exptime)
        if obstime is not None:
            try:
                obstime = pfsTime.Time.fromisoformat(str(obstime))
            except Exception as e:
                cmd.warn(f'text="FAILED to parse obstime={obstime}: {e}"')
                obstime = None
        else:
            try:
                obstime = self.read0StartStamp
            except:
                pass

        fullRampTime = actortime.TimeCards(startTime=obstime)

        frameTime = self.calcFrameTime()
        rampExptime = self.nread*frameTime
        darktime = rampExptime

        # The spsActor may tell us what the real exposure time is expected to be. Use that if available
        # and we are not just taking a dark.
        if exptype.lower() != 'dark' and 'expectedExptime' in cmdKeys:
            exptime = float(cmdKeys['expectedExptime'].values[0])
        elif exptime is None:
            exptime = rampExptime

        fullRampTime.end(expTime=exptime)

        timecards = []
        timecards.append(dict(name='EXPTIME', value=exptime))
        timecards.append(dict(name='DARKTIME', value=darktime))
        timecards.extend(fullRampTime.getCards())

        t1 = time.time()
        if t1 - t0 > 1:
            cmd.warn(f'text="it took {t1-t0:0.2f} seconds to fetch time cards!"')
        return timecards, exptime

    def getPfsHeader(self, visit=None,
                     exptype='TEST',
                     objname=None, obstime=None,
                     fullHeader=True, cmd=None):

        allCards = []
        if fullHeader:
            allCards.append(dict(name='DATA-TYP',
                                 value=exptype.upper(),
                                 comment='Subaru-style exposure type'))

        if fullHeader:
            self.hdrMgr = hdrMgr = spsFits.SpsFits(self.actor, cmd, exptype)

            timeCards, exptime = self.getTimeCards(cmd=cmd, exptype=exptype,
                                                   obstime=obstime)

            newCards = hdrMgr.finishHeaderKeys(cmd, visit,
                                               timeCards, exptime)
            allCards.extend(newCards)

            hxCards = self.genAllH4Cards(cmd)
            allCards.extend(hxCards)
            if self.actor.ids.site == 'J':
                allCards.extend(self.genJhuCards(cmd))

            if objname is not None:
                allCards.append(dict(name='OBJECT',
                                     value=objname,
                                     comment='user-specified name'))

            # mhsCards = self._getMhsHeader(cmd)
            # if objname is not None:
            #     mhsCards = [c for c in mhsCards if c['name'] != 'OBJECT']

        else:
            allCards.append(dict(name='INHERIT', value=True))
            allCards.extend(wcs.pixelWcsCards())
        hxReadCards = self._getHxHeader(cmd)
        allCards.extend(hxReadCards)

        keep = []
        for c in allCards:
            try:
                _ = pickle.dumps(c)
                keep.append(c)
            except:
                cmd.warn(f'text="dropping bad card: {c}"')
        allCards = keep
        return allCards

    def getResetHeader(self, cmd):
        allCards = []

        allCards.append(dict(name='INHERIT', value=True))
        allCards.extend(self._getHxHeader(cmd))
        return allCards

    def reloadLogic(self, cmd):
        self.sam.reloadLogic()
        cmd.finish()

    def setReadSpeed(self, cmd):
        cmdKeys = cmd.cmd.keywords

        style = 'fast' if 'fast' in cmdKeys else 'fast'
        logLevel = logging.DEBUG if 'debug' in cmdKeys else logging.INFO

        link = self.sam.link
        link.configureReadout(style=style, logLevel=logLevel)
        cmd.inform(f'text="readStyle {link.readCheckInterval} {link.readChunkSize}"')
        cmd.finish()
