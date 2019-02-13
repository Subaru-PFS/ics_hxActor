#!/usr/bin/env python

import logging

import actorcore.ICC

try:
    from pfscore import spectroIds
    instrument = "PFS"
except ImportError:
    instrument = "CHARIS"

class OurActor(actorcore.ICC.ICC):
    def __init__(self, name,
                 productName=None,
                 camName=None,
                 imageCamName=None,
                 debugLevel=30):

        """ Setup an Actor instance. See help for actorcore.Actor for details. """
        
        self.instrument = instrument
        if instrument == 'PFS':
            if imageCamName is None:
                imageCamName = camName
            self.spectroIds = spectroIds.SpectroIds(partName=imageCamName)
            if camName is None:
                camName = self.spectroIds.camName
            specName = self.spectroIds.specName
            name = f"hx_{camName}"
            modelNames = ("hx_{camName}", "xcu_{camName}", "enu_{specName}")
        else:
            name = "hx"
            modelNames = ('hx',)
            
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        print(f'configuring for {name}, with models={modelNames}, instrument={instrument}')
        actorcore.ICC.ICC.__init__(self, name, 
                                   productName=productName,
                                   modelNames=modelNames)

        self.everConnected = False

    def connectionMade(self):
        if self.everConnected is False:
            self.logger.info(f'ids: {self.spectroIds.idDict}')
            self.logger.info("Attaching all controllers...")
            self.allControllers = [s.strip() for s in self.config.get(self.name, 'startingControllers').split(',')]
            self.attachAllControllers()
            self.everConnected = True
        

#
# To work
def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--logLevel', default=logging.INFO, type=int, nargs='?',
                        help='logging level')
    parser.add_argument('--cam', default=None, nargs='?',
                        help='camera name')
    parser.add_argument('--imageCam', default=None, nargs='?',
                        help='camera name for image files')
    args = parser.parse_args()
    
    theActor = OurActor(None,
                        productName='hxActor',
                        camName=args.cam,
                        imageCamName=args.imageCam)
    theActor.run()

if __name__ == '__main__':
    main()
