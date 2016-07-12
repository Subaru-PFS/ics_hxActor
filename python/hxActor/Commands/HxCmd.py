#!/usr/bin/env python

import time

import astropy.io.fits as pyfits

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

import hxActor.winFiles
reload(hxActor.winFiles)

class HxCmd(object):

    def __init__(self, actor):
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
            ('flush', '', self.flush),
            ('ramp', '[<nramp>] [<nreset>] [<nread>] [<ngroup>] [<ndrop>] [@splitRamps]', self.takeRamp),
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
                                        )

        self.rampConfig = None
        
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
        expTime = 1.5
        if expType == 'ramp':
            return expTime * (self.rampConfig['nread'] +
                              self.rampConfig['nreset'] +
                              self.rampConfig['ndrop'])
        elif expType == 'single':
            return expTime * (1 + self.rampConfig['nreset'])
        elif expType == 'CDS':
            return expTime * (2 + self.rampConfig['nreset'])
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


    def flush(self, cmd, doFinish=True):
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
            while rampsDone < nramp:
                event = fileQ.get(timeout=timeLimits[0])

                cmd.diag('text="filesys event: %s"' % (event))
                fileOrDir, action, path = event.split()

                if fileOrDir == 'file' and action == 'done':
                    cmd.inform('text="new read (%d/%d in ramp %d/%d): %s"' % (readsDone+1,nread,
                                                                              rampsDone+1,nramp,
                                                                              path))
                    readsDone += 1
                    
                if readsDone >= nread:
                    rampsDone += 1
                    readsDone = 0
            cmd.inform('text="ramps done (%d/%d)"' % (rampsDone+1,nramp))
        except Exception as e:
            cmd.warn('winfile readers failed with %s' % (e))
            fileAlerts.terminate()
    
    def takeRamp(self, cmd):
        cmdKeys = cmd.cmd.keywords

        nramp = cmdKeys['nramp'].values[0] if ('nramp' in cmdKeys) else 1
        nreset = cmdKeys['nreset'].values[0] if ('nreset' in cmdKeys) else 1
        nread = cmdKeys['nread'].values[0] if ('nread' in cmdKeys) else 1
        ndrop = cmdKeys['ndrop'].values[0] if ('ndrop' in cmdKeys) else 0
        ngroup = cmdKeys['ngroup'].values[0] if ('ngroup' in cmdKeys) else 1
        
        cmd.diag('text="ramps=%s resets=%s reads=%s rdrops=%s rgroups=%s"' %
                 (nramp, nreset, nread, ndrop, ngroup))

        dosplit = 'splitRamps' in cmdKeys
        nrampCmds = nramp if dosplit else 1

        cmd.inform('text="configuring ramp..."')
        cmd.inform('ramp=%d,%d,%d,%d,%d' % (nramp,ngroup,nreset,nread,ndrop))

        self.flush(cmd, doFinish=False)
        
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
            
            
        

        
                
