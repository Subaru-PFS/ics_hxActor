import queue
import threading
import time

import numpy as np

from ics.utils import mhs
from ics.utils import time as pfsTime

def isoTs(t=None):
    if t is None:
        ts = pfsTime.Time.now()
    else:
        ts = pfsTime.Time.fromtimestamp(t)

    return ts.isoformat()
class flusher(threading.Thread):
    def __init__(self, cmd, visit, filename=None):
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
        self.filename = filename if filename is not None else f'/tmp/PFXA{visit:06d}03.fits'

        self.writeDelay = 2.0
        self.closeDelay = 5.0

    def delayWrite(self):
        # Needs to be some distribution TBD
        time.sleep(self.writeDelay)

    def delayClose(self):
        # Needs to be some distribution TBD
        time.sleep(self.closeDelay)

    def exit(self):
        self.q.put(('exit', None))
    def finishCmd(self):
        self.q.put(('finish', None))
    def writeCmd(self, groupn, readn):
        self.q.put(('write', (groupn,readn)))

    def run(self):
        while True:
            action, args = self.q.get()
            if action == 'exit':
                self.cmd.inform('text="sim I/O thread exiting')
                return
            elif action == 'finish':
                t0 = time.time()
                self.delayClose()
                t1 = time.time()
                self.cmd.inform(f'filename={self.filename}')
                return
            elif action == 'write':
                t0 = time.time()
                groupn,readn = args
                self.delayWrite()
                t1 = time.time()
                self.cmd.inform(f'hxwrite={self.visit},{groupn},{readn},{t1-t0:0.2f}')

def rampSim(cmd, visit, nread, ngroup=1, nreset=1, ndrop=0, readTime=10.857):
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
    cmd.inform('rampConfig=%d,%d,%d,%d,%d' % (visit,ngroup,nreset,nread,ndrop))

    # How long before the ASIC gets to the top of the next frame: a random fraction of
    # a full read time.
    startDelay = np.random.random(1)[0] * readTime

    # Wait for the ASIC, then tell the world about the expected times.
    time.sleep(startDelay)
    resetStart = time.time()
    read0Start = resetStart + nreset*readTime

    resetStartStamp = isoTs(resetStart)
    read0StartStamp = isoTs(read0Start)
    cmd.inform(f'readTimes={visit},{resetStartStamp},{read0StartStamp},{readTime:0.3f}')

    ioThread = flusher(cmd, visit)
    ioThread.start()

    try:
        # need to be sloppier: the hxread etc outputs come after the file I/O has been done.
        for i in range(nreset):
            time.sleep(readTime)
            cmd.inform(f'hxread={visit},0,{i+1}')
            ioThread.writeCmd(0,i+1)
        for g_i in range(ngroup):
            for i in range(nread):
                time.sleep(readTime)
                cmd.inform(f'hxread={visit},{g_i+1},{i+1}')
                ioThread.writeCmd(1,i+1)
            for d_i in range(ndrop):
                time.sleep(readTime)
                cmd.inform(f'text="ignoring drop read {d_i+1}/{ndrop} "')
        ioThread.finishCmd()
    finally:
        ioThread.join()

    cmd.finish()

