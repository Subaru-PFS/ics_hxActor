import os.path
import threading
import time

class NightFilenameGen(object):
    def __init__(self, rootDir='.',
                 seqnoFile='nextSeqno', 
                 genFilesFunc=None,
                 filePrefix='TEST', fileSuffix="fits",
                 filePattern="%(filePrefix)s%(seqno)08d.%(fileSuffix)s",
                 dayOffset=-3600*12):

        """ Set up a per-night filename generator.
        
        Under a given root, each night gets a subdirectory, and all the files inder the root 
        are named using a managed sequence number. For example:
          /data/PFS
          /data/PFS/2014-04-02/PFSA00000012.fits
          /data/PFS/2014-04-03/PFSA00000013.fits

        We do _not_ create any files, except for the directories and a seqno file.

        Parameters
        ----------
        rootDir - string
          The root directory that we manage. Will be created if 
          it does not exist.
        seqnoFile - string, optional
          The name of the file where we save the next sequence number.
        genFilesFunc - callable, optional
          A function which takes (directoryName, sequenceNumber), and
          returns a list of complete paths. 
        filePrefix - string, optional, default="TEST"
        filePattern - string, optional, default="%(filePrefix)s%(seqno)08d.%(fileSuffix)",
        dayOffset - integer, optional, default=3600*12
          The night's rollover time. By default, noon UT.
        """
        
        self.rootDir = rootDir
        self.filePrefix = filePrefix
        self.filePattern = filePattern
        self.fileSuffix = fileSuffix
        self.namesFunc = self.defaultNamesFunc

        self.simRoot = None
        self.simSeqno = None
        self.dayOffset = dayOffset

        head, tail = os.path.split(seqnoFile)
        if not head:
            seqnoFile = os.path.join(rootDir, tail)
        self.seqnoFile = seqnoFile

        self.seqnoFileLock = threading.Lock()

        self.setup()
        
    def setup(self, rootDir=None, seqnoFile=None, seqno=1):
        """ If necessary, create directories and sequence files. """

        if not rootDir: 
            rootDir = self.rootDir
        if not seqnoFile: 
            seqnoFile = self.seqnoFile
        
        if not os.path.isdir(rootDir):
            os.makedirs(rootDir)


        if not os.access(seqnoFile, os.F_OK):
            seqFile = open(seqnoFile, "w")
            seqFile.write("%d\n" % (seqno))

    def defaultNamesFunc(self, rootDir, seqno):
        """ Returns a list of filenames. """ 

        d = dict(filePrefix=self.filePrefix, seqno=seqno, fileSuffix=self.fileSuffix)
        filename = os.path.join(rootDir, self.filePattern % d)
        return (filename,)
                                
    def consumeNextSeqno(self):
        """ Return the next free sequence number. """

        with self.seqnoFileLock:
            try:
                sf = open(self.seqnoFile, "r")
                seq = sf.readline()
                seq = seq.strip()
                seqno = int(seq)
            except Exception, e:
                raise RuntimeError("could not read sequence integer from %s: %s" %
                                   (self.seqnoFile, e))

            nextSeqno = seqno+1
            try:
                sf = open(self.seqnoFile, "w")
                sf.write("%d\n" % (nextSeqno))
                sf.truncate()
                sf.close()
            except Exception, e:
                raise RuntimeError("could not WRITE sequence integer to %s: %s" %
                                   (self.seqnoFile, e))

        return nextSeqno

    def dirname(self):
        """ Return the next directory to use. """

        dirnow = time.time() + self.dayOffset
        utday = time.strftime('%Y-%m-%d', time.gmtime(dirnow))

        dataDir = os.path.join(self.rootDir, utday)
        if not os.path.isdir(dataDir):
            # cmd.respond('text="creating new directory %s"' % (dataDir))
            os.mkdir(dataDir, 0o2775)

        return dataDir
    
    def genNextRealPath(self):
        """ Return the next filename to create. """

        dataDir = self.dirname()
        seqno = self.consumeNextSeqno()
        imgFiles = self.namesFunc(dataDir, seqno)
        
        return imgFiles
        
    def genNextSimPath(self):
        """ Return the next filename to read. """

        filenames = self.namesFunc(self.simRoot, self.simSeqno)
        self.simSeqno += 1
    
        return filenames if os.path.isfile(filenames[0]) else None

    def getNextFileset(self):
        if self.simRoot:
            return self.genNextSimPath()
        else:
            return self.genNextRealPath()

def test1():
    # def __init__(self, rootDir, seqnoFile, filePrefix='test', namesFunc=None):

    gen = FilenameGen('/tmp', 'testSeq')
    gen.setup()
    
