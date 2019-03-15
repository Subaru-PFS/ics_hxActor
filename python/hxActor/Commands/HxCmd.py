#!/usr/bin/env python

import os.path
import time

import fitsio
import astropy.io.fits as pyfits

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr
import actorcore.utility.fits as actorFits

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
            ('getVoltages', '', self.getVoltages),
            ('getSpiRegisters', '', self.getSpiRegisters),
            ('getTelemetry', '', self.getTelemetry),
            ('getAsicPower', '', self.getAsicPower),
            ('getAsicErrors', '', self.getAsicErrors),
            ('resetAsic', '', self.resetAsic),
            ('powerOffAsic', '', self.powerOffAsic),
            ('powerOnAsic', '', self.powerOnAsic),
            ('setVoltage', '<voltageName> <voltage>', self.setVoltage),
            ('ramp',
             '[<nramp>] [<nreset>] [<nread>] [<ngroup>] [<ndrop>] [<itime>] [@splitRamps] [<seqno>] [<exptype>] [<objname>]',
             self.takeRamp),
            ('reloadLogic', '', self.reloadLogic),
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
                                                 help='number of readss to take.'),
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
                                        keys.Key("voltageName", types.String(), default=None,
                                                 help='voltage name'),
                                        keys.Key("voltage", types.Float(), default=None,
                                                 help='voltage'),
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
                # Write the full ramp
                fileName = self.actor.spectroIds.makeFitsName(visit=seqno, fileType='B')
                return os.path.join(dataRoot, fileName),
            
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
        """Set a songle Hx bias voltage. """
        
        if self.backend is not 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        voltageName = cmdKeys['voltageName'].values[0]
        voltage = cmdKeys['voltage'].values[0]
        
        sam = self.sam

        try:
            newVoltage = sam.setBiasVoltage(voltageName, voltage)
        except Exception as e:
            cmd.fail('text="Failed to set voltage %s=%s: %s"' % (voltageName,
                                                                 voltage,
                                                                 e))
        
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
        volts, amps = self.sam.telemetry()
        cmd.finish('text="see log for telemetry"')
    
    def getAsicPower(self, cmd):
        V,A,W = self.sam.readAsicPower()

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

        if self.backend == 'hxhal':
            t0 = time.time()
            sam = self.sam
            sam.fileGenerator = self.fileGenerator
            
            def readCB(ramp, group, read, filename, image):
                cmd.inform('hxread=%s,%d,%d,%d' % (filename, ramp, group, read))
                if nread == read:
                    cmd.inform('filename=%s' % (filename))

            def headerCB(ramp, group, read, seqno):
                if self.actor.instrument == 'CHARIS':
                    hdr = self.getCharisHeader(seqno=seqno, fullHeader=(read == 1), cmd=cmd)
                    return hdr.cards
                elif self.actor.instrument == 'PFS':
                    return []
                else:
                    raise RuntimeError(f'actor.instrument is not a known device: {self.actor.instrument}')

            def pfsHeaderCB(ramp, group, read, seqno):
                return []
            
            filenames = sam.takeRamp(nResets=nreset, nReads=nread, noReturn=True, nRamps=nramp,
                                     seqno=seqno, exptype=exptype,
                                     headerCallback=pfsHeaderCB,
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
            

    def reloadLogic(self, cmd):
        self.sam.reloadLogic()
        cmd.finish()
        
