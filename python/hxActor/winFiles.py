import logging
import multiprocessing
import os
import sys
import time

import inotify.adapters

def trackWinDir(rootDir, q, logger=None, timeLimit=None):
    """ Generate notifications of directory and file events from the Teledyne IDL software..

    Specifically, we expect `rootDir` to be the per-detectory directory which contains 
    the UpTheRamp, CDSReference, Reference directories.
    
    As it stands we only watch the UpTheRamp directory, but that might change.
 
    Args:
       rootDir (str) : the root of the directory tree to watch.
       q (Queue)     : the queue to write notifications to.

    """

    if logger is None:
        logger = logging

    sectionsToWatch = {'UpTheRamp'}
    subDirs = dict()
    filesDone = dict()
    
    i = inotify.adapters.Inotify()
    i.add_watch(rootDir)
    for d in sectionsToWatch:
        i.add_watch(os.path.join(rootDir, d))

    lastTime = time.time()
    for event in i.event_gen():
        if event is None:
            thisTime = time.time()
            if thisTime - lastTime > timeLimit:
                raise RuntimeError('timeout (>%s sec) listening for image files.' % (thisTime-lastTime))
            lastTime = thisTime
            continue

        (header, events, watch_path, filename) = event

        filepath = os.path.join(watch_path, filename)
        if 'IN_ISDIR' in events:
            if 'IN_CREATE' in events:
                if filename[:3] == '201':
                    lastWatch = subDirs.get(watch_path, None)
                    if lastWatch is not None:
                        q.put('dir done %s' % (lastWatch))
                        i.remove_watch(lastWatch)
                    subDirs[watch_path] = filepath
                    i.add_watch(filepath)
                    q.put('dir add %s' % (filepath))
                    continue
            elif 'IN_CLOSE_NOWRITE' in events:
                continue
        elif 'IN_CREATE' in events and filename.startswith('H2RG_'):
            q.put('file add %s' % (filepath))
            continue
        elif 'IN_CLOSE_WRITE' in events and filename.startswith('H2RG_'):
            if filepath not in filesDone:
                filesDone[filepath] = True
                q.put('file done %s' % (filepath))
            continue
        
        if 'IN_MODIFY' not in events:
            logger.debug("WD=(%d) MASK=(%d) COOKIE=(%d) LEN=(%d) MASK->NAMES=%s "
                         "WATCH-PATH=[%s] FILENAME=[%s]",
                         header.wd, header.mask, header.cookie, header.len, events,
                         watch_path, filename)

class FileAlert(multiprocessing.Process):
    def __init__(self, topDir, logger=None, timeLimit=15):
        super(FileAlert, self).__init__(name="FileAlert(%s)" % (topDir))
        self.daemon = True
        self.topDir = topDir
        self.q = multiprocessing.Queue()
        self.timeLimit = timeLimit
        if logger is None:
            self.logger = logging.getLogger('winFiles')
        self.logger.debug('inited process %s' % (self.name))
    
    def run(self):
        self.logger.info('starting process %s' % (self.name))
        os.chdir(self.topDir)
        
        trackWinDir(self.topDir, self.q, self.logger, timeLimit=self.timeLimit)
        
def main():
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(20)

    if len(sys.argv) > 1:
        root = sys.argv[1]
    else:
        root = os.path.realpath(os.path.curdir)
        
    f = FileAlert(root, logger=logger)
    f.start()
    while True:
        ev = f.q.get()
        print ev
    
if __name__ == "__main__":
    main()
