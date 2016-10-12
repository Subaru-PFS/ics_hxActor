import logging
import multiprocessing
import socket

import astropy.io.fits as pyfits

headerAddr = 'localhost', 6666

def fetchHeader(frameid=9999, mode=1, itime=0.0):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    query = "%d %d %0.2f\n" % (frameid, mode, itime)
    logging.info("sending query: %s ", query[:-1])
    try:
        # Connect to server and send data
        sock.connect(headerAddr)
        sock.sendall(query)
    except Exception as e:
        logging.error("failed to send: %s" % (e))
        received = ""
        return pyfits.Header()

    logging.info("sent query: %s ", query[:-1])
    try:
        received = ""
        while True:
            # Receive data from the server and shut down
            oneBlock = sock.recv(2880)
            logging.info("received: ", oneBlock)
            received = received + oneBlock

            if oneBlock.strip().endswith('END'):
                break

    except Exception as e:
        logging.error("failed to read: %s" % (e))
        received = ""
    finally:
        sock.close()

    logging.info("final received: ", oneBlock)
    hdr = pyfits.Header.fromstring(received)

    logging.info("read %d bytes, %0.4f blocks, header len=%d" % (len(received), len(received) / 2880.0, len(hdr)))

    return hdr

class FetchHeader(multiprocessing.Process):
    def __init__(self, logger=None, timeLimit=15, frameId=9999, itime=0.0):
        super(FetchHeader, self).__init__(name="FetchHeader")
        self.daemon = True
        self.q = multiprocessing.Queue()
        self.timeLimit = timeLimit
        self.frameId = frameId
        self.itime = itime
        if logger is None:
            self.logger = logging.getLogger('fetchHeader')
            self.logger.setLevel(logging.DEBUG)
            
        self.logger.debug('inited process %s' % (self.name))
    
    def run(self):
        self.logger.info('starting process %s' % (self.name))

        try:
            hdrString = fetchHeader(self.frameId, mode=1, itime=self.itime)
        except Exception as e:
            self.logger.warn('fetchHeader failed: %s', e)
            self.q.put(pyfits.Header().tostring())
            return
        
        self.logger.info('header: %s' % (len(hdrString)))
        
        self.q.put(hdrString)
        