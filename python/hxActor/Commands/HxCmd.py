#!/usr/bin/env python

import os.path
import re
import time

import fitsio

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

import hxActor.winFiles
reload(hxActor.winFiles)

class HxCmd(object):

    def __init__(self, actor):
        self.readTime = 1.47528
        
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('win', '@raw', self.winRaw),
            ('bounce', '', self.bounce),
            ('getconfig', '', self.winGetconfig),
            ('single', '', self.takeSingle),
            ('cds', '', self.takeCDS),
            ('flush', '', self.flushProgramInput),
            ('ramp', '[<nramp>] [<nreset>] [<nread>] [<ngroup>] [<ndrop>] [<itime>] [@splitRamps]', self.takeRamp),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("xcu_play", (1, 1),
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
                                        )

        self.rampConfig = None

        self.dataRoot = "/home/data/charis"
        self.dataPrefix = "CRSA"
        
        from utils import seqPath
        self.fileGenerator = seqPath.NightFilenameGen(self.dataRoot,
                                                      filePrefix=self.dataPrefix)
        
    @property
    def controller(self):
        return self.actor.controllers['winjarvis']

    def bounce(self, cmd):
        self.controller.disconnect()
        
    def winRaw(self, cmd):
        cmdKeys = cmd.cmd.keywords

        ctrl = self.actor.controllers['winjarvis']

        rawCmd = cmdKeys['raw'].values[0]
        cmd.diag('text="sending raw: %s"' % (rawCmd))
        ret = ctrl.sendOneCommand(rawCmd, cmd=cmd)
        cmd.finish('text="raw: %s"' % (ret))

    def winGetconfig(self, cmd, doFinish=True):
        ctrl = self.actor.controllers['winjarvis']

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
        
        if expType == 'ramp':
            return self.readTime * (self.rampConfig['nread'] +
                                    self.rampConfig['nreset'] +
                                    self.rampConfig['ndrop'])
        elif expType == 'single':
            return self.readTime * (1 + self.rampConfig['nreset'])
        elif expType == 'CDS':
            return self.readTime * (2 + self.rampConfig['nreset'])
        else:
            raise RuntimeError("unknown expType %s" % (expType))
        
    def takeSingle(self, cmd):
        if self.rampConfig is None:
            self.winGetconfig(cmd, doFinish=False)
        ret = self.controller.sendOneCommand('acquireSingleFrame',
                                             cmd=cmd,
                                             timeout=self._calcAcquireTimeout(expType='single'))
        cmd.finish('text="%s"' % (ret))

    def takeCDS(self, cmd):
        if self.rampConfig is None:
            self.winGetconfig(cmd, doFinish=False)
        ret = self.controller.sendOneCommand('acquirecds',
                                             cmd=cmd,
                                             timeout=self._calcAcquireTimeout(expType='CDS'))
        cmd.finish('text="%s"' % (ret))


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

    def consumeRead(self, path, cmd):
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
            self.outfile = self.fileGenerator.getNextFileset()[0]
            cmd.diag('text="new filename %s"' % (self.outfile))
            cards = [dict(name='IDLPATH', value=dirName)]
            phdu = fitsio.FITSHDR(cards)
            fitsio.write(self.outfile, None, header=phdu, clobber=True)
            cmd.diag('text="new file %s"' % (self.outfile))
            
        inData, inHdr = fitsio.read(path, header=True)
        stackFile = fitsio.FITS(self.outfile, mode='rw')
        stackFile.write(inData, header=inHdr)
        stackFile[-1].write_checksum()
        stackFile.close()
        cmd.inform('readN=%d,%d,%d,%s' % (rampN,groupN,readN,self.outfile))
    
    def consumeRamps(self, nramp, ngroup, nreset, nread, ndrop, cmd, timeLimits=None):
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
            while rampsDone < nramp:
                event = fileQ.get(timeout=timeLimits[0])

                cmd.diag('text="filesys event: %s"' % (event))
                fileOrDir, action, path = event.split()

                if fileOrDir == 'file' and action == 'done':
                    cmd.debug('text="new read (%d/%d in ramp %d/%d): %s"' % (readsDone+1,nread,
                                                                             rampsDone+1,nramp,
                                                                             path))
                    readsDone += 1
                    self.consumeRead(path, cmd)
                    
                if readsDone >= nread:
                    rampsDone += 1
                    readsDone = 0
                    cmd.inform('filename=%s' % (self.outfile))
                    self.outfile = None
                    if rampsDone < nramp:
                        cmd.inform('text="ramp %d/%d starting %d resets..."' % (rampsDone+1, nramp, nreset))
                        
        except Exception as e:
            cmd.warn('winfile readers failed with %s' % (e))
            fileAlerts.terminate()

        self.outfile = None
        
    def takeRamp(self, cmd):
        cmdKeys = cmd.cmd.keywords

        nramp = cmdKeys['nramp'].values[0] if ('nramp' in cmdKeys) else 1
        nreset = cmdKeys['nreset'].values[0] if ('nreset' in cmdKeys) else 1
        nread = cmdKeys['nread'].values[0] if ('nread' in cmdKeys) else 1
        ndrop = cmdKeys['ndrop'].values[0] if ('ndrop' in cmdKeys) else 0
        ngroup = cmdKeys['ngroup'].values[0] if ('ngroup' in cmdKeys) else 1
        itime = cmdKeys['itime'].values[0] if ('itime' in cmdKeys) else None
        
        cmd.diag('text="ramps=%s resets=%s reads=%s rdrops=%s rgroups=%s itime=%s"' %
                 (nramp, nreset, nread, ndrop, ngroup, itime))

        if itime is not None:
            if 'nread' in cmdKeys:
                cmd.fail('text="cannot specify both nread= and itime="')
                return
            nread = int(itime / self.readTime) + 1
        
        dosplit = 'splitRamps' in cmdKeys
        nrampCmds = nramp if dosplit else 1

        if nread * nramp * ngroup == 0:
            cmd.fail('text="all of nramp,ngroup,(nread or itime) must be positive"')
            return
        
        cmd.inform('text="configuring ramp..."')
        cmd.inform('ramp=%d,%d,%d,%d,%d' % (nramp,ngroup,nreset,nread,ndrop))

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
            self.consumeRamps((1 if dosplit else nramp),
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
            
