#!/usr/bin/env python

from importlib import reload

import os.path
import time

import fitsio
import astropy.io.fits as pyfits

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr
from actorcore.utility import fits as fitsUtils

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
            ('hxconfig', '[<configName>]', self.hxconfig),
            ('getVoltage', '<name>', self.sampleVoltage),
            ('getVoltages', '', self.getVoltages),
            ('getSpiRegisters', '', self.getSpiRegisters),
            ('getRefCal', '', self.getRefCal),
            ('getTelemetry', '', self.getTelemetry),
            ('getAsicPower', '', self.getAsicPower),
            ('getAsicErrors', '', self.getAsicErrors),
            ('resetAsic', '', self.resetAsic),
            ('powerOffAsic', '', self.powerOffAsic),
            ('powerOnAsic', '', self.powerOnAsic),
            ('setVoltage', '<name> <voltage>', self.setVoltage),
            ('ramp',
             '[<nramp>] [<nreset>] [<nread>] [<ngroup>] [<ndrop>] [<itime>] [@splitRamps] [<seqno>] [<exptype>] [<objname>]',
             self.takeRamp),
            ('reloadLogic', '', self.reloadLogic),
            ('readAsic', '<reg> [<nreg>]', self.getAsicReg),
            ('readSam', '<reg> [<nreg>]', self.getSamReg),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("hx", (1, 2),
                                        keys.Key("seqno", types.Int(), default=None,
                                                 help='If set, the assigned sequence number.'),
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
                                        keys.Key("exptype", types.String(), default=None,
                                                 help='What to put in IMAGETYP/DATA-TYP.'),
                                        keys.Key("objname", types.String(), default=None,
                                                 help='What to put in OBJECT.'),
                                        keys.Key("configName", types.String(), default=None,
                                                 help='configuration name'),
                                        keys.Key("name", types.String(), default=None,
                                                 help='voltage name'),
                                        keys.Key("voltage", types.Float(), default=None,
                                                 help='voltage'),
                                        keys.Key("lamp", types.Int(), default=0,
                                                 help='lamp name'),
                                        keys.Key("lampPower", types.Int(), default=0,
                                                 help='lamp power (0..1023)xs'),
                                        keys.Key("reg", types.String(),
                                                 help='register number (hex or int)'),
                                        keys.Key("nreg", types.Int(), default=1,
                                                 help='number of registers to read'),
                                        )

        self.backend = 'hxhal'
        self.rampConfig = None

        if self.actor.instrument == "CHARIS":
            self.dataRoot = "/home/data/charis"
            self.dataPrefix = "CRSA"
            filenameFunc = None
        else:
            self.dataRoot = "/data/pfsx"
            self.dataPrefix = "PFJA"

            def filenameFunc(dataRoot, seqno):
                """ Return a pair of filenames, one for the ramp, one for the single stack image. """
                
                # Write the full ramp
                fileNameA = self.actor.ids.makeSpsFitsName(visit=seqno, fileType='A')
                fileNameB = self.actor.ids.makeSpsFitsName(visit=seqno, fileType='B')
                return os.path.join(dataRoot, fileNameA), os.path.join(dataRoot, fileNameB)
            
        from hxActor.charis import seqPath
        self.fileGenerator = seqPath.NightFilenameGen(self.dataRoot,
                                                      namesFunc = filenameFunc,
                                                      filePrefix=self.dataPrefix)
        
    @property
    def controller(self):
        return self.actor.controllers.get(self.backend, None)

    @property
    def sam(self):
        ctrlr = self.actor.controllers.get(self.backend, None)
        return ctrlr.sam

    def bounce(self, cmd):
        self.controller.disconnect()

    def hxconfig(self, cmd):
        """Set the given hxhal configuration. """
        
        if self.backend is not 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        configName = cmdKeys['configName'].values[0]
        
        sam = self.sam

        try:
            configGroup, configName = configName.split('.')
        except:
            configGroup = 'h4rgConfig' if self.actor.instrument == 'PFS' else 'h2rgConfig'
            
        sam.updateHxRgConfigParameters(configGroup, configName)
        cmd.finish()
        
    def setVoltage(self, cmd):
        """Set a single Hx bias voltage. """
        
        if self.backend is not 'hxhal' or self.controller is None:
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
        cmd.finish(f'text="set {voltageName} to {newVoltage:.3f}"')

    def _sampleVoltage(self, cmd, voltageName, doFinish=True):
        sam = self.sam

        try:
            volt, raw = sam.sampleVoltage(voltageName)
        except Exception as e:
            cmd.fail('text="Failed to sample voltage %s: %s"' % (voltageName,
                                                                 e))
            return
        
        setting = sam.getBiasVoltage(voltageName)

        cmdFunc = cmd.finish if doFinish else cmd.inform
        cmdFunc(f'text="{voltageName:12s} = {volt: .3f} set {setting: .3f}, raw {raw:#04x}"')
        
    def sampleVoltage(self, cmd, doFinish=True):
        """Sample a single Hx bias voltage. """
        
        if self.backend is not 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        voltageName = str(cmdKeys['name'].values[0])

        self._sampleVoltage(cmd, voltageName, doFinish=doFinish)
        
    def getVoltages(self, cmd):
        sam = self.sam

        self.getRefCal(cmd, doFinish=False)
        for vname in sam.voltageNames:
            self._sampleVoltage(cmd, vname, doFinish=False)
            
        cmd.finish()
    
    def getRefCal(self, cmd, doFinish=True):
        """Sample the ASIC refence offset and gain. """
        
        if self.backend is not 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        sam = self.sam
        sam._buildVoltageTable()

        try:
            aduPerVolt, aduOffset = sam.calibrateRefOffsetAndGain()
        except Exception as e:
            cmd.fail('text="Failed to fetch reference calibration %s"' % (e))

        cmdFunc = cmd.finish if doFinish else cmd.inform
        cmdFunc(f'text=" offset={aduOffset:#04x}/{aduOffset}; ADU/V={aduPerVolt} uV/ADU={1e6/aduPerVolt:0.1f}"')
        
    def getAsicReg(self, cmd):
        """Read ASIC register(s). """
        
        if self.backend is not 'hxhal' or self.controller is None:
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
        
    def getSamReg(self, cmd):
        """Read SAM/Jade register(s). """
        
        if self.backend is not 'hxhal' or self.controller is None:
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

        frameTime = self.sam.frameTime
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

        itime = self.sam.frameTime
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
                  exptype='TEST', objname='TEST',
                  timeout=1.0, cmd=None):
        try:
            hdr = self.getSubaruHeader(frameId, fullHeader=fullHeader,
                                       exptype=exptype,
                                       timeout=timeout, cmd=cmd)
        except Exception as e:
            self.logger.warn('text="failed to fetch Subaru header: %s"' % (e))
            cmd.warn('text="failed to fetch Subaru header: %s"' % (e))
            hdr = pyfits.Header()

        hdr.set('OBJECT', objname, before=1)
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
    
    def getVoltages(self, cmd):
        ret = self.sam.getBiasVoltages()
        for nv in ret:
            name, voltage = nv
            cmd.inform('%s=%0.3f' % (name, voltage))
        cmd.finish()
    
    def getSpiRegisters(self, cmd):
        h4Regs = self.sam.readAllH4SpiRegs()
        for i, reg in enumerate(h4Regs):
            cmd.inform(f'spiReg%d=0x%04x' % (i, reg))
        cmd.finish()

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

        nBanks = 4
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
        
        cmd.finish('text="see log for telemetry"')
        
    def lamp(self, lamp, lampPower, cmd):
        if self.actor.ids.camName != 'n8':
            return

        from hxActor.Commands import opticslab
        reload(opticslab)

        opticslab.lampCmd(lamp, lampPower)
        cmd.inform('text="lamp %s=%s"' % (lamp, lampPower))

    def _addLampCard1(self, lamp, lampPower):
        if self.actor.ids.camName != 'n8':
            return

        self.hxCards['W_OLLAMP'] = pyfits.Card('W_OLLAMP', lamp, 'Optics lab lamp')
        self.hxCards['W_OLLPLV'] = pyfits.Card('W_OLLPLV', lampPower, 'Optics lab lamp command level')
        
    def getLastState(self, lamp, lampPower, cmd):
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
        self.hxCards['W_OLLAMP'] = pyfits.Card('W_OLLAMP', lamp, 'Optics lab lamp')
        self.hxCards['W_OLLPLV'] = pyfits.Card('W_OLLPLV', lampPower, 'Optics lab lamp command level')
        self.hxCards['W_OLLPWV'] = pyfits.Card('W_OLLPWV', lam, '[nm] Lamp center wavelength')
        self.hxCards['W_OLLPCR'] = pyfits.Card('W_OLLPCR', current, '[A] Photodiode current')
        self.hxCards['W_OLFLUX'] = pyfits.Card('W_OLFLUX', flux, '[photons/s] Calibrated flux')
    
    def takeRamp(self, cmd):
        """Main exposure entry point. 
        """
        
        cmdKeys = cmd.cmd.keywords

        nramp = cmdKeys['nramp'].values[0] if ('nramp' in cmdKeys) else 1
        nreset = cmdKeys['nreset'].values[0] if ('nreset' in cmdKeys) else 1
        nread = cmdKeys['nread'].values[0] if ('nread' in cmdKeys) else 1
        ndrop = cmdKeys['ndrop'].values[0] if ('ndrop' in cmdKeys) else 0
        ngroup = cmdKeys['ngroup'].values[0] if ('ngroup' in cmdKeys) else 1
        itime = cmdKeys['itime'].values[0] if ('itime' in cmdKeys) else None
        seqno = cmdKeys['seqno'].values[0] if ('seqno' in cmdKeys) else None
        exptype = cmdKeys['exptype'].values[0] if ('exptype' in cmdKeys) else 'TEST'
        objname = cmdKeys['objname'].values[0] if ('objname' in cmdKeys) else 'TEST'
        lamp = cmdKeys['lamp'].values[0] if ('lamp' in cmdKeys) else 0
        lampPower = cmdKeys['lampPower'].values[0] if ('lampPower' in cmdKeys) else 0
        outputReset = 'outputReset' in cmdKeys

        self.lamp(0, 0, cmd)
        
        cmd.diag('text="ramps=%s resets=%s reads=%s rdrops=%s rgroups=%s itime=%s seqno=%s exptype=%s"' %
                 (nramp, nreset, nread, ndrop, ngroup, itime, seqno, exptype))

        if itime is not None:
            if 'nread' in cmdKeys:
                cmd.fail('text="cannot specify both nread= and itime="')
                return
            nread = int(itime / self.sam.frameTime) + 1
        
        dosplit = 'splitRamps' in cmdKeys
        nrampCmds = nramp if dosplit else 1

        if nread <= 0 or nramp <= 0 or ngroup <= 0:
            cmd.fail('text="all of nramp,ngroup,(nread or itime) must be positive"')
            return
        
        cmd.inform('text="configuring ramp..."')
        cmd.inform('ramp=%d,%d,%d,%d,%d' % (nramp,ngroup,nreset,nread,ndrop))
        self.hxCards = dict()
        self._addLampCard1(lamp, lampPower)
        
        if self.backend == 'hxhal':
            t0 = time.time()
            sam = self.sam
            sam.fileGenerator = self.fileGenerator
            
            def readCB(ramp, group, read, filename, image,
                       lamp=lamp, lampPower=lampPower):
                cmd.inform('hxread=%s,%d,%d,%d' % (filename, ramp, group, read))
                if read == 0 or group == 0 and read == nreset:
                    if lampPower != 0:
                        self.lamp(lamp, lampPower, cmd)
                if read == nread-1:
                    self.getLastState(lamp, lampPower, cmd)
                if read == nread:
                    if lampPower != 0:
                        self.lamp(0, 0, cmd)
                    cmd.inform('filename=%s' % (filename))

            def headerCB(ramp, group, read, seqno):
                if self.actor.instrument == 'CHARIS':
                    hdr = self.getCharisHeader(seqno=seqno, fullHeader=(read == 1), cmd=cmd)
                    return hdr.cards
                elif self.actor.instrument == 'PFS':
                    hdr = self.getPfsHeader(seqno=seqno, fullHeader=(read == 1), cmd=cmd)
                    return hdr
                else:
                    raise RuntimeError(f'actor.instrument is not a known device: {self.actor.instrument}')

            filenames = sam.takeRamp(nResets=nreset, nReads=nread,
                                     noReturn=True, nRamps=nramp,
                                     seqno=seqno, exptype=exptype,
                                     headerCallback=headerCB,
                                     readCallback=readCB)
        else:    
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
        """ Gather FITS cards from all actors we are interested in. """

        cmd.debug('text="fetching MHS cards..."')
        cards = fitsUtils.gatherHeaderCards(cmd, self.actor, shortNames=True)
        cmd.debug('text="fetched %d MHS cards..."' % (len(cards)))

        # Until we convert to fitsio, convert cards to pyfits
        pycards = []
        for c in cards:
            if isinstance(c, str):
                pcard = 'COMMENT', c
            else:
                pcard = c['name'], c['value'], c.get('comment', '')
            pycards.append(pcard)
            cmd.debug('text=%s' % (qstr("fetched card: %s" % (str(pcard)))))

        return pycards

    
    def _getHxHeader(self, cmd):
        """ Gather FITS cards from ourselves. """

        cmd.debug('text="fetching HX cards..."')
        cards = self.hxCards.values()
        cmd.debug('text="fetched %d HX cards..."' % (len(cards)))

        return cards

    def getPfsHeader(self, seqno=None,
                     exptype='TEST',
                     fullHeader=True, cmd=None):

        mhsCards = self._getMhsHeader(cmd)
        hxCards = self._getHxHeader(cmd)
        mhsCards.extend(hxCards)
        return mhsCards
    
        
    def reloadLogic(self, cmd):
        self.sam.reloadLogic()
        cmd.finish()
        
