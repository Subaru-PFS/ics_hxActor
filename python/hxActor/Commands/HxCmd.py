#!/usr/bin/env python

import time

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

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
            ('ramp', '[<nramp>] [<nreset>] [<nread>] [<ngroup>] [<ndrop>] [@nosplit]', self.takeRamp),
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

        kterms = []
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

    def _calcAcquireTimeout(self, expType='ramp', cmd=None):
        expTime = 1.5
        extraTime = 10
        if expType == 'ramp':
            return extraTime + expTime * (self.rampConfig['nread'] +
                                          self.rampConfig['nreset'])
        elif expType == 'single':
            return extraTime + expTime * (1 + self.rampConfig['nreset'])
        elif expType == 'CDS':
            return extraTime + expTime * (2 + self.rampConfig['nreset'])
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

    def takeRamp(self, cmd):
        cmdKeys = cmd.cmd.keywords

        nramp = cmdKeys['nramp'].values[0] if ('nramp' in cmdKeys) else 1
        nreset = cmdKeys['nreset'].values[0] if ('nreset' in cmdKeys) else 1
        nread = cmdKeys['nread'].values[0] if ('nread' in cmdKeys) else 1
        ndrop = cmdKeys['ndrop'].values[0] if ('ndrop' in cmdKeys) else 0
        ngroup = cmdKeys['ngroup'].values[0] if ('ngroup' in cmdKeys) else 1
        
        cmd.diag('text="ramps=%s resets=%s reads=%s rdrops=%s rgroups=%s"' %
                 (nramp, nreset, nread, ndrop, ngroup))

        nosplit = 'nosplit' in cmdKeys
        nrampCmds = 1 if nosplit else nramp
        
        ctrl = self.controller
        ret = self.controller.sendOneCommand('setRampParam(%d,%d,%d,%d,%d)' %
                                             (nreset,nread,ngroup,ndrop,nramp if nosplit else 1),
                                             cmd=cmd)
        self.winGetconfig(cmd, doFinish=False)
        timeout = self._calcAcquireTimeout(expType='ramp')
        if nosplit:
            timeout *= nramp
        t0 = time.time()
        for r_i in range(nrampCmds):
            t0_0 = time.time()
            cmd.inform('text="ramp %d of %d"' % (r_i+1, nramp))
            ret = self.controller.sendOneCommand('acquireramp',
                                                 cmd=cmd,
                                                 timeout=timeout)
            dtRamp = time.time() - t0_0
            cmd.inform('text="%s, rampTime=%0.4f, perRead=%0.4f"' %
                       (ret, dtRamp, dtRamp / (nread + nreset))
            )

        t1 = time.time()
        dt = t1-t0
        cmd.finish('text="%d ramps, elapsed=%0.4f, perRamp=%0.4f, perRead=%0.4f"' %
                   (nramp, dt, dt/nramp, dt/(nramp*(nread+nreset))))
            
            
        

        
                
