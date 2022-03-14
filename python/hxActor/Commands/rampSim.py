import queue
import threading
import time

import numpy as np

from ics.utils import time as pfsTime

def isoTs0(t=None, tz=time.timezone, precision=3):
    """An ISO-formatted local timestamp, based on UTC unix seconds.

    Parameters
    ----------
    t : `float`, optional
        a time.time(). If None, make one now.
    tz : `int`, optional
        the timezone to use, by default the host's.
        If None: use the local timezone, but do not format it.
    precision : int, optional
        how many digits for fractional seconds, by default 3

    Returns
    -------
    timestamp : `str`
       An ISO 8601 compliant time string
    """
    if t is None:
        t = time.time()

    if precision == 0:
        fracStr = ''
    else:
        fracSeconds = int((t%1) * 10**precision)
        fracStr = f'.{fracSeconds:0{precision}d}'

    if tz is None:
        tzname=''
    elif tz == 0:
        tzname = 'Z'
    else:
        tzname = "%z"

    return time.strftime(f'%Y-%m-%dT%H:%M:%S{fracStr}{tzname}',
                         time.localtime(t))

def isoTs(t=None):
    if t is None:
        ts = pfsTime.Time.now()
    else:
        ts = pfsTime.Time.fromtimestamp(t)

    return ts.isoformat()

class NullCmd:
    """Dummy Command for when we are not using actorcore code."""
    def print(self, level, s):
        print(f'{isoTs()} {level} {s}')

    def diag(self, s):
        self.print('d', s)
    def inform(self, s):
        self.print('i', s)
    def warn(self, s):
        self.print('w', s)
    def finish(self, s=""):
        self.print(':', s)
    def fail(self, s=""):
        self.print('f', s)

class flusher(threading.Thread):
    def __init__(self, cmd, visit, filename='/tmp/nonexistent.fits', rampn=1):
        """Simulate file writes with delays

        Parameters
        ----------
        cmd : `Command`
            What we send responses to.
        visit : int
            The PFS visit
        filename : `str`
            The filename we would have written to.
        rampn : `int`, optional
            Which ramp we are writing out., by default 1
        """
        threading.Thread.__init__(self, daemon=True)
        self.cmd = cmd
        self.q = queue.Queue()
        self.visit = visit
        self.filename = filename

        self.writeDelay = 0.5
        self.closeDelay = 5.0

        self.rampn = rampn
        self.readn = 0

    def delayWrite(self):
        # Needs to be some distribution TBD
        time.sleep(self.writeDelay)

    def delayClose(self):
        # Needs to be some distribution TBD
        time.sleep(self.closeDelay)

    def exit(self):
        self.q.put(('exit', Nonels )
    def finishCmd(self):
        self.q.put('finish')
    def writeCmd(self, readn, groupn, rampn=1):
        self.q.put('write')

    def run(self):
        while True:
            action, args = self.q.get()
            if action == 'exit':
                self.cmd.inform('text="sim I/O thread exiting')
                return
            elif action == 'finish':
                self.delayClose()
                self.cmd.inform(f'filename={self.filename}')
            elif action == 'write':
                self.delayWrite()
                self.cmd.inform(f'hxwrite={self.visit},{self.rampn},{self.readn},1')
                self.readn += 1

def rampSim(cmd, visit, nread, nramp=1, ngroup=1, nreset=1, ndrop=0, readTime=10.857):
    """Simulate the keywords and timing of taking a ramp

    The ASIC always runs its own internal line clock. Our reads sync to that,
    so the announcement of the reset comes between 0..readTime seconds after
    the ramp has been commanded. After that, real DAQ timing is absolutely
    isochronous, but the file writing times are sloppier.

    Parameters
    ----------
    cmd : `Command`
        What we reply to.
    visit : `int`
        The PFS visit
    nread : `int`
        number of reads in the ramp.
    readTime : `float`
        seconds per read.
    """

    # Declare the shape of our output.
    cmd.inform('ramp=%d,%d,%d,%d,%d' % (nramp,ngroup,nreset,nread,ndrop))

    # How long before the ASIC starts feeding us the reset frame.
    startDelay = np.random.random(1) * readTime

    # Wait for the ASIC, then tell the world about the expected times.
    time.sleep(startDelay)
    resetStart = time.time()
    read0Start = resetStart + nreset*readTime

    resetStartStamp = isoTs(resetStart)
    read0StartStamp = isoTs(read0Start)
    cmd.inform(f'{resetStart} {read0Start} {read0Start-resetStart}')
    cmd.inform(f'readTimes={resetStartStamp},{read0StartStamp},{readTime:0.3f}')

    ioThread = flusher(cmd, visit, '/tmp/foo')
    ioThread.start()

    try:
        # need to be sloppier: the hxread etc outputs come after the file I/O has been done.
        for i in range(nreset):
            time.sleep(readTime)
            ioThread.writeCmd()
        for i in range(nread):
            time.sleep(readTime)
            self.cmd.inform(f'hxread={visit},{self.rampn},{self.readn},1')
            ioThread.writeCmd()
        ioThread.finishCmd()

    finally:
        ioThread.exit()
        ioThread.join()

    cmd.finish()

