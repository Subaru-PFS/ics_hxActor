#!/usr/bin/env python

import logging

import actorcore.ICC

try:
    from pfs.utils import spectroIds
    instrument = "PFS"
except ImportError:
    instrument = "CHARIS"

class OurActor(actorcore.ICC.ICC):
    def __init__(self, name,
                 productName=None,
                 camName=None,
                 debugLevel=30):

        """ Setup an Actor instance. See help for actorcore.Actor for details. """

        self.instrument = instrument
        if instrument == 'PFS':
            self.ids = spectroIds.SpectroIds(partName=camName)
            if name is None:
                name = f"hx_{self.ids.camName}"
            modelNames = ()
        else:
            name = "hx"
            modelNames = ('hx', 'charis')
            
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        print(f'configuring for {name}, camera={self.ids.camName} instrument={instrument}')
        actorcore.ICC.ICC.__init__(self, name, 
                                   productName=productName,
                                   modelNames=modelNames)
        # For engineering, where the piepan might not be the same as the camera
        self.piepanName = self.ids.camName
        try:
            imageCamName = self.config.get(self.name, 'imageCamName')
            self.ids = spectroIds.SpectroIds(partName=imageCamName)
            self.logger.warning(f'RECONFIGURED for imageCam {imageCamName}')
        except Exception as e:
            self.logger.info("not overwriting imageCam: %s", e)

        self.everConnected = False

    def connectionMade(self):
        if self.everConnected is False:
            self.logger.info(f'{self.name} ids: {self.ids.idDict}')
            self.logger.info("Attaching all controllers...")
            self.allControllers = [s.strip() for s in self.config.get(self.name, 'startingControllers').split(',')]
            self.attachAllControllers()
            self.everConnected = True

            models = ['gen2', 'pfilamps']
            models.extend([m % self.ids.idDict for m in ('hx_%(camName)s', 'enu_%(specName)s')])
            models.extend(['dcb', f'xcu_{self.piepanName}'])

            if self.ids.idDict['cam'] == 'n1':
                models.append('idg')
                
            self.logger.info('adding models: %s', models)
            self.addModels(models)
            self.logger.info('added models: %s', self.models.keys())
#
# To work
def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--logLevel', default=logging.INFO, type=int, nargs='?',
                        help='logging level')
    parser.add_argument('--cam', default=None, nargs='?',
                        help='camera name')
    args = parser.parse_args()
    
    theActor = OurActor(None,
                        productName='hxActor',
                        camName=args.cam)
    theActor.run()

if __name__ == '__main__':
    main()
