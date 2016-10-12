import errno
import logging
import select
import socket

def _eintr_retry(func, *args):
    """restart a system call interrupted by EINTR"""
    while True:
        try:
            return func(*args)
        except (OSError, select.error) as e:
            if e.args[0] != errno.EINTR:
                raise

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
        
        if cmd is not None:
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
    
    def getOneChar(self, s=None, timeout=2, cmd=None):
        if s is None:
            s = self.connect(cmd=cmd)

        readers, writers, broken = _eintr_retry(select.select, [s], [], [], timeout)
        if len(readers) == 0:
            if cmd is not None:
                cmd.warn('text="Timed out reading character from %s controller"' % (self.name))
            raise RuntimeError('timeout')
        return s.recv(1)

    def getOneResponse(self, s=None, cmd=None, timeout=5):
        if s is None:
            s = self.connect(cmd=cmd)

        ret = ''
        while not ret.endswith(self.EOL):
            c = self.getOneChar(s, cmd=cmd, timeout=(timeout if not ret else None))
            ret += c

        ret = ret[:-len(self.EOL)]
        cmd.diag('text="raw from %s: %r"' % (self.name, ret))
        return ret
    
    def sendOneCommand(self, cmdStr, cmd=None, timeout=None, noResponse=False):
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

        if not noResponse:
            try:
                ret = self.getOneResponse(self.sock, cmd=cmd, timeout=timeout) 
            except Exception as e:
                cmd.warn('text="failed to read response from %s: %s"' % (self.name, e))
                self.sock = None
                raise

            self.logger.debug('received %r', ret)
            cmd.diag('text="received %r"' % ret)
        else:
            ret = None
            
        return ret

