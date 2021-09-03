from importlib import reload

import logging
import numpy as np
import socket
import time

from fpga import opticslab
reload(opticslab)

logger = logging.getLogger('illuminati')
logger.setLevel(logging.DEBUG)

def illuminatorCommand(cmdStr, timeout=5.0):
    host = 'illuminati.pfs'
    port = 6563

    logger.info('illuminator command: %s', cmdStr)
    
    try:
        s = socket.create_connection((host, port), 3.0)
    except Exception:
        return None

    t0 = time.time()
    try:
        s.settimeout(timeout)
        ss = '%s\n' % (cmdStr)
        s.send(ss.encode('latin-1'))
        data = s.recv(1024)
        data = data.strip()
    finally:
        s.close()
    t1 = time.time()

    logger.debug("cmd: %r dt=%0.2f", cmdStr, t1-t0)
    
    return data.decode('latin-1')

"""
Murdock sez:
For GPIO 20 and 26
00 is channel B 1050 nm
10 is channel C 1200 nm
01 is channel A 940 nm
11 is channel D 1300 nm

To get photons/s/pixel you will need to multiply the monitor current by
 940 nm - 4.09515675e+12
1050 nm -  4.12424927e+12
1200 nm -  4.82236340e+12
1300 nm - 6.47803190e+12

Craig replies:
OK, GPIO ("BCM") 20 and 26 are, respectively, BOARD 38 and 37, and
illuminator.py uses 37/26 as the high bit. So the mapping from the
existing illuminator.py channel IDs to your measured channels is:

id hi lo lamp
0  0 0   B/1050
1  0 1   C/1200
2  1 0   A/940
3  1 1   D/1300

"""
lamps = {0:(1050, 4.12424927e+12),
         1:(1200, 4.82236340e+12),
         2:(940, 4.09515675e+12),
         3:(1300, 6.47803190e+12)}

def getFlux(lamp=None):
    current = opticslab.getCurrent()
    if lamp is None:
        return current, np.nan, np.nan

    lam, scale = lamps[lamp]
    return current, lam, current*scale

def lampCmd(lamp, level):
    if lamp is None or level == 0:
        cmdStr = 'off'
    else:
        cmdStr = f'on {lamp} {level}'

    illuminatorCommand(cmdStr)
