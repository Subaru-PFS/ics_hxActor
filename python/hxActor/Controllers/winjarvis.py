import logging
import select
import socket
import sys
import time

import numpy as np

class winjarvis(object):
    def __init__(self, actor, name,
                 loglevel=logging.INFO):

        self.actor = actor
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)

        self.EOL = '\r\n'

        self.host = self.actor.config.get(self.name, 'host')
        self.port = int(self.actor.config.get(self.name, 'port'))

        self.sock = None
        
    def start(self):
        pass

    def stop(self, cmd=None):
        pass

    def disconnect(self):
        self.sock = None
        
    def connect(self, cmd, force=False):
        if self.sock is not None and not force:
            return self.sock
        
        cmd.inform('text="connecting socket to %s..."' % (self.name))
        self.sock = None
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
        except socket.error as e:
            cmd.warn('text="failed to create socket to %s: %s"' % (self.name, e))
            raise
 
        try:
            s.connect((self.host, self.port))
        except socket.error as e:
            cmd.warn('text="failed to connect to %s: %s"' % (self.name, e))
            s.close()
            raise

        self.sock = s
        return s
    
    def getOneChar(self, s, timeout=2, cmd=None):
        readers, writers, broken = select.select([s.fileno()], [], [], timeout)
        if len(readers) == 0:
            cmd.warn('text="Timed out reading character from %s controller"' % (self.name))
            raise RuntimeError('timeout')
        return s.recv(1)

    def getOneResponse(self, s, cmd=None, timeout=5):
        ret = ''
        while not ret.endswith(self.EOL):
            c = self.getOneChar(s, cmd=cmd, timeout=(timeout if not ret else None))
            ret += c

        ret = ret[:-len(self.EOL)]
        cmd.diag('text="raw from %s: %r"' % (self.name, ret))
        return ret
    
    def sendOneCommand(self, cmdStr, cmd=None, timeout=None):
        if cmd is None:
            cmd = self.actor.bcast

        fullCmd = "%s%s" % (cmdStr, self.EOL)
        self.logger.debug('sending %r', fullCmd)
        cmd.diag('text="sending %r with timeout=%s"' % (cmdStr, timeout))

        self.connect(cmd=cmd)
        
        try:
            self.sock.sendall(fullCmd)
        except socket.error as e:
            cmd.warn('text="failed to send to %s: %s"' % (self.name, e))
            self.sock = None
            raise

        try:
            ret = self.getOneResponse(self.sock, cmd=cmd, timeout=timeout) 
        except socket.error as e:
            cmd.warn('text="failed to read response from %s: %s"' % (self.name, e))
            self.sock = None
            raise

        self.logger.debug('received %r', ret)
        cmd.diag('text="received %r"' % ret)

        return ret

