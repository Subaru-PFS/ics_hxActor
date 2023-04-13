import logging
import pathlib

from ics.utils.fits import fitsWriter

class Ramp(object):
    def __init__(self, cmd, reportReads=True, logLevel=logging.INFO):
        """A per-ramp object which relays completed FITS events to the MHS command. """
        self.logger = logging.getLogger('ramp')
        self.logger.setLevel(logLevel)
        self.cmd = cmd
        self.name = f'ramp_{id(self):#08x}'
        self.reportReads = reportReads
        self.isFinished = False

    def createdFits(self, reply):
        if reply['status'] != 'OK':
            msg = f'failed to create FITS file {reply["path"]}: {reply["errorDetails"]}'
            self.cmd.warn(msg)
            self.logger.warning(msg)
            return

        self.logger.info(f'{self.name}: createdFits: {reply}')
        self.cmd.inform(f'text="created {reply["path"]}')

    def amendedPHDU(self, reply):
        if reply['status'] != 'OK':
            msg = f'failed to amend PDHU: {reply["errorDetails"]}'
            self.cmd.warn(msg)
            self.logger.warning(msg)
            return

        self.logger.info(f'{self.name}: amendedPHDU: {reply}')
        self.cmd.inform(f'text="amended PHDU for current file')

    def wroteHdu(self, reply):
        """A read has been written to the FITS file. """
        if reply['status'] != 'OK':
            msg = f'failed to append HDU to FITS file {reply["path"]}: {reply["errorDetails"]}'
            self.cmd.warn(msg)
            self.logger.warning(msg)
            return

        ramp, group, read = reply['hduId']
        self.logger.info(f'{self.name} wroteHdu: {reply}')
        if self.reportReads and read is not None:
            path = pathlib.Path(reply['path'])
            self.cmd.inform('hxread=%d,%d,%d,%d' % (int(path.stem[4:-2], base=10),
                                                    ramp, group, read))

    def closedFits(self, reply):
        """The FITS file has been closed and renamed to the final pathname. """
        if reply['status'] != 'OK':
            msg = f'failed to close FITS file {reply["path"]}: {reply["errorDetails"]}'
            self.cmd.warn(msg)
            self.logger.warning(msg)
            return

        self.logger.info(f'{self.name} closedFits: {reply}')
        self.cmd.inform('filename=%s' % (reply['path']))
        self.isFinished = True

    def fitsFailure(self, reply):
        self.logger.warning(f'{self.name} fitsFailure: {reply}')
        self.cmd.warn(f'failure with FITS file {reply["path"]}: {reply["errorDetails"]}')



