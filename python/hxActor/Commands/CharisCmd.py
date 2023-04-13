#!/usr/bin/env python

from __future__ import print_function
from builtins import range
from builtins import object

import os.path
import re
import time

import fitsio
import astropy.io.fits as pyfits

import opscore.protocols.keys as keys
import opscore.protocols.types as types

try:
    import actorcore.utility.fits as actorFits
    import hxActor.charis.winFiles as winFiles
    import hxActor.charis.subaru as subaru
    import hxActor.charis.scexao as scexao
    reload(winFiles)
    reload(subaru)
    reload(scexao)
except:
    pass

class CharisCmd(object):

    def __init__(self, actor):
        
        # This lets us access the rest of the actor.
        self.actor = actor
        self.logger = self.actor.logger

        if self.actor.instrument != 'CHARIS':
            self.logger.info('not CHARIS, skipping CharisCmd.py')
            self.vocab = []
            self.keys = keys.KeysDictionary('charis', (1, 1))
            return
        
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('backend', '@(windows|unix)', self.setBackend),
            ('win', '@raw', self.winRaw),
            ('wingetconfig', '', self.winGetconfig),
            ('winflush', '', self.flushProgramInput),
            ('charisConfig', '', self.charisConfig),
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

        self.dataRoot = "/home/data/charis"
        self.dataPrefix = "CRSA"
        
        from hxActor.charis import seqPath
        self.fileGenerator = seqPath.NightFilenameGen(self.dataRoot,
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
        
        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        configName = cmdKeys['configName'].values[0]
        
        sam = self.sam
        
        sam.updateHxRgConfigParameters('h2rgConfig', configName)
        cmd.finish()
        
    def charisConfig(self, cmd):
        if self.backend != 'hxhal' or self.controller is None:
            cmd.fail('text="No hxhal controller"')
            return

        cmdKeys = cmd.cmd.keywords
        sam = self.sam
        
        cmd.inform('text="setting ASIC configuration...."')
        sam.updateHxRgConfigParameters('h2rgConfig', 'cold_feb_05')

        cmd.inform('text="setting voltages...."')
        sam.link.WriteAsicReg(0x602c,0x82c3)

        cmd.finish()
        
    def setBackend(self, cmd):
        """Select the backend to use. 

        Really not sure how to do this. Doing it right is impossible
        (you might need to steal/hand off the /dev/QUSB device between
        W7 and Unix. Among other horrors.)

        I think all this should do is:
          1. declare (via .backend), which backend we want.
          2. try (once) to (dis/re)-connect  
        """
        
        cmdKeys = cmd.cmd.keywords
        if 'unix' in cmdKeys:
            want = 'hxhal'
        elif 'windows' in cmdKeys:
            want = 'winjarvis'
        elif 'none' in cmdKeys:
            want = None

        runningBackend = self.backend
        self.backend = None

        self.backend = want

        cmd.finish()
        return
    
        if self.controller is not None:
            cmd.inform('text="disconnecting from backend: %s"' % (runningBackend))
            try:
                self.actor.detachController(runningBackend)
            except Exception as e:
                cmd.warn("text='failed to disconnect: %s'" % (e))

        worked = self.actor.attachController(want)
        if worked:
            self.backend = want
            cmd.finish()
        else:
            cmd.fail()
        
    def winRaw(self, cmd):
        """ Tunnel a rawCmd command to these Windows IDL program. """
        
        cmdKeys = cmd.cmd.keywords

        ctrl = self.controller

        rawCmd = cmdKeys['raw'].values[0]
        cmd.diag('text="sending raw: %s"' % (rawCmd))
        ret = ctrl.sendOneCommand(rawCmd, cmd=cmd)
        cmd.finish('text="raw: %s"' % (ret))

    def winGetconfig(self, cmd, doFinish=True):
        """ Fetch the results of these Windows IDL 'getconfiguration' command. """

        ctrl = self.controller

        ret = ctrl.sendOneCommand('getconfig', cmd=cmd)
        ret = ret.replace('nOutputs', ' nOutputs')
        ret = ret.replace('winXStop', ' winXStop')
        ret = ret.replace('winYStart', ' winYStart')
        parts = ret.split()

        self.rampConfig = dict()
        for p in parts:
            k,v = p.split('=')
            k = k.lower()
            if k in {'nresets','nreads','nramps','ndrops','ngroups'}:
                print("config %s ->(%s, %s)" % (p, k[:-1], v))
                self.rampConfig[k[:-1]] = int(v)
                
        kparts = '; '.join(['win_%s' % p for p in parts])
        cmd.inform(kparts)
        if doFinish:
            cmd.finish()
        return parts
    
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

    def _consumeRead(self, path, cmd, header=None):
        #  /home/data/wincharis/H2RG-C17206-ASIC-104/UpTheRamp/20160712210126/H2RG_R01_M01_N01.fits
        dirName, fileName = os.path.split(path)
        cmd.diag('text="checking %s"' % (fileName))
        match = re.match('^H2RG_R0*(\d+)_M0*(\d+)_N0*(\d+)\.fits', fileName)
        if match is None:
            cmd.warn("failed to split up filename: %s" % (file))
            return
        rampN, groupN, readN = [int(m) for m in match.group(1,2,3)]
        cmd.diag('text="new read %d %d %d"' % (rampN, groupN, readN))
        if readN == 1:
            if header is not None:
                cmd.diag('text="getting header"')
                subaruHdr = header
            else:
                subaruHdr = pyfits.Header()
            cards = [dict(name='IDLPATH', value=dirName)]
            for c in subaruHdr.cards:
                cards.append(dict(name=c.keyword, value=c.value, comment=c.comment))
            phdu = fitsio.FITSHDR(cards)
            fitsio.write(self.outfile, None, header=phdu, clobber=True)
            cmd.diag('text="new file %s"' % (self.outfile))
            
        inData, inHdr = fitsio.read(path, header=True)
        stackFile = fitsio.FITS(self.outfile, mode='rw')
        stackFile.write(inData, header=inHdr)
        stackFile[-1].write_checksum()
        stackFile.close()
        cmd.inform('readN=%d,%d,%d,%s' % (rampN,groupN,readN,self.outfile))

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

    def getCharisCards(self, cmd):
        charisModel = self.actor.models['charis'].keyVarDict
        cards = []

        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'grism', 'Y_GRISM', idx=0,
                                               comment='deprecated: grism position'))
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'grism', 'Y_PRISM', idx=0,
                                               comment='prism position'))
        # Was CHARIS.FILTER.NAME
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'filterSlot', 'Y_FLTNAM', idx=1,
                                               comment='current filter name'))
        # Was CHARIS.FILTER.SLOT
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'filterSlot', 'Y_FLTSLT', cnv=int, idx=0,
                                               comment='current filter slot'))
        # Was CHARIS.SHUTTER
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'shutter', 'Y_SHUTTR', idx=1,
                                               comment='shutter position'))
        # Was CHARIS.LASER.ENABLED
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'laserState', 'Y_LSRENB', cnv=bool, idx=0,
                                               comment='is laserState enabled'))
        # Was CHARIS.LASER.POWER
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'laserState', 'Y_LSRPWR', idx=2,
                                               comment='laser power, percent'))
        # Was CHARIS.LASER.ALARMS
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'laserState', 'Y_LSRALM', idx=3,
                                               comment='laser alarms'))
        # Was CHARIS.TEMPS.%d
        for i in range(10):
            cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                                   'temps', 'Y_TEMP%02d'%i, idx=i,
                                                   comment='temperature sensor %d'%i))

        # Add global Subaru aliases
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'filterSlot', 'FILTER01', idx=1,
                                               comment='current filter name'))
        cards.append(actorFits.makeCardFromKey(cmd, charisModel,
                                               'grism', 'DISPERSR', idx=0,
                                               comment='prism position'))
        return cards
        
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

    def getCharisHeader(self, seqno=None,
                        exptype='TEST',
                        fullHeader=True, cmd=None):
        return self.getHeader(seqno, exptype=exptype,
                              fullHeader=fullHeader, cmd=cmd)
    
    def getHxCards(self, cmd=None):
        # voltageList = self.controller.getAllBiasVoltages
        return []
    
    def getSCExAOCards(self, cmd=None):
        hdr = []
        try:
            hdr = scexao.fetchHeader()
        except:
            pass

        return hdr
    
    def _consumeRamps(self, nramp, ngroup, nreset, nread, ndrop, cmd, timeLimits=None):
        if timeLimits is None:
            timeLimits = (nreset*1.5+15,
                          nread*1.5+10)

        root = '/home/data/wincharis/H2RG-C17206-ASIC-104'
        cmd.debug('text="starting winFiles i=on %s with timeout=%s"' % (root, timeLimits[0]))
        fileAlerts = hxActor.winFiles.FileAlert(root, timeLimit=timeLimits[0])
        fileQ = fileAlerts.q
        fileAlerts.start()

        try:
            rampsDone = 0
            readsDone = 0
            cmd.inform('text="ramp %d/%d starting %d resets..."' % (rampsDone+1, nramp, nreset))
            self.outfile = self.fileGenerator.getNextFileset()[0]
            cmd.inform('text="new filename %s"' % (self.outfile))
            try:
                header = self.getHeader(self.fileGenerator.seqno, nread*self.readTime, cmd=cmd)
                cmd.debug('text="header process returned %s"' % (None if header is None else len(header)))
            except Exception as e:
                cmd.warn('text="failed to start header process: %s"' % (e))
                header = None
            
            while rampsDone < nramp:
                if readsDone == 0:
                    cmd.debug('text="waiting for filesys event...')
                event = fileQ.get(timeout=timeLimits[0])

                cmd.debug('text="filesys event: %s"' % (event))
                fileOrDir, action, path = event.split()

                if fileOrDir == 'file' and action == 'done':
                    cmd.debug('text="new read (%d/%d in ramp %d/%d): %s"' % (readsDone+1,nread,
                                                                             rampsDone+1,nramp,
                                                                             path))
                    readsDone += 1
                    self._consumeRead(path, cmd, header)
                    
                if readsDone >= nread:
                    rampsDone += 1
                    readsDone = 0
                    cmd.inform('filename=%s' % (self.outfile))
                    self.outfile = None
                    if rampsDone < nramp:
                        cmd.inform('text="ramp %d/%d starting %d resets..."' % (rampsDone+1, nramp, nreset))
                        self.outfile = self.fileGenerator.getNextFileset()[0]
                        cmd.diag('text="new filename %s"' % (self.outfile))
                        try:
                            header = self.getHeader(self.fileGenerator.seqno, nread*self.readTime, cmd=cmd)
                        except Exception as e:
                            cmd.warn('text="failed to start header process: %s"' % (e))
                            header = None
                        
        except Exception as e:
            cmd.warn('winfile readers failed with %s' % (e))
            fileAlerts.terminate()
        finally:
            fileAlerts.terminate()
            del fileAlerts
            
        self.outfile = None

