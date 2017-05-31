import logging
import multiprocessing
import socket

import astropy.io.fits as pyfits

headerAddr = 'rhodey', 6666

def fetchSeqno(prefix='A', instrument='CRS'):
    """ Request frame_id from Gen2. """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    query = "seqno %s\n" % (prefix)
    logging.info("sending query: %s ", query[:-1])
    try:
        # Connect to server and send data
        sock.connect(headerAddr)
        sock.sendall(query)
    except Exception as e:
        logging.error("failed to send: %s" % (e))
        received = ""
        return '%s%s%0*d' % (instrument, prefix, 9-len(prefix), 9999)

    logging.debug("sent query: %s ", query[:-1])
    try:
        received = ""
        while True:
            # Receive data from the server and shut down
            oneBlock = sock.recv(1024)
            logging.debug("received: %s", oneBlock)
            received = received + oneBlock

            if len(received) >= 12:
                break

    except Exception as e:
        logging.error("failed to read: %s" % (e))
        received = ""
    finally:
        sock.close()

    logging.debug("final received: %s", received)
    return received

def fetchHeader(fullHeader=True, frameid=9999, mode=1, itime=0.0):
    """Request FITS cards from the Gen2 side. """

    try:
        gen2Frameid = "CRSA%08d" % (frameid)
    except:
        gen2Frameid = 'None'
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    query = "hdr %s %s %0.2f %s\n" % (gen2Frameid, mode, itime, fullHeader)
    logging.info("sending query: %s ", query[:-1])
    try:
        # Connect to server and send data
        sock.connect(headerAddr)
        sock.sendall(query)
    except Exception as e:
        logging.error("failed to send: %s" % (e))
        received = ""
        return pyfits.Header()

    logging.debug("sent query: %s ", query[:-1])
    try:
        received = ""
        while True:
            # Receive data from the server and shut down
            try:
                oneBlock = sock.recv(2880)
                logging.debug("received: %s", oneBlock)
                received = received + oneBlock
            except socket.error, e:
                if e.errno != errno.EINTR:
                    raise
    
            if received.strip().endswith('END'):
                break

    except Exception as e:
        logging.error("failed to read: %s" % (e))
        received = ""
    finally:
        sock.close()

    logging.debug("final received: %s", len(received) / 80.0)
    hdr = pyfits.Header.fromstring(received)

    logging.info("read %d bytes, %0.4f blocks, header len=%d" % (len(received), len(received) / 2880.0, len(hdr)))

    return hdr

class FetchHeader(multiprocessing.Process):
    def __init__(self, logger=None, fullHeader=True, timeLimit=15, frameId=9999, itime=0.0):
        super(FetchHeader, self).__init__(name="FetchHeader")
        self.daemon = True
        self.q = multiprocessing.Queue()
        self.timeLimit = timeLimit
        self.frameId = frameId
        self.itime = itime
        self.fullHeader = fullHeader
        if logger is None:
            self.logger = logging.getLogger('fetchHeader')
            self.logger.setLevel(logging.DEBUG)
            
        self.logger.debug('inited process %s (frameId=%s)' % (self.name, frameId))
    
    def run(self):
        self.logger.info('starting process %s (%s)' % (self.name, self.frameId))

        try:
            hdr = fetchHeader(self.fullHeader,self.frameId, mode=1, itime=self.itime)
            hdrString = hdr.tostring()
        except Exception as e:
            self.logger.warn('fetchHeader failed: %s', e)
            self.q.put(pyfits.Header().tostring())
            return
        
        self.logger.info('header: %s' % (len(hdrString)))
        
        self.q.put(hdrString)
        
