#!/usr/bin/env python

import logging

import actorcore.ICC
from ics.utils import instdata

try:
    from ics.utils.sps import spectroIds
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
        print(f'configuring for {name}, camera={self.ids.camName} instrument={instrument} ids={self.ids.idDict}')
        actorcore.ICC.ICC.__init__(self, name, 
                                   productName=productName,
                                   modelNames=modelNames,
                                   idDict=self.ids.idDict)
        # For engineering, where the piepan might not be the same as the camera
        self.piepanName = self.ids.camName

        self.simulateOnly = self.actorConfig.get('simulator', False)

        try:
            imageCamName = self.actorConfig['imageCamName']
            self.ids = spectroIds.SpectroIds(partName=imageCamName)
            self.logger.warning(f'RECONFIGURED for imageCam {imageCamName}')
        except Exception as e:
            self.logger.info("not overwriting imageCam: %s", e)
        self.logger.info(f'ids: {self.ids.idDict}')

        self.everConnected = False

    @property
    def enuModel(self):
        enuName = 'enu_%(specName)s' % self.ids.idDict
        return self.models[enuName]

    @property
    def xcuModel(self):
        xcuName = f'xcu_{self.piepanName}'
        return self.models[xcuName]

    def connectionMade(self):
        if self.everConnected is False:
            if self.simulateOnly:
                self.logger.info("Attaching self as ramp command simulator only")
                self.everConnected = True
                return

            self.logger.info(f'{self.name} ids: {self.ids.idDict}')
            self.logger.info("Attaching all controllers...")
            self.allControllers = self.actorConfig['controllers']['starting']
            self.attachAllControllers()
            self.everConnected = True

            models = ['sunss', 'gen2', 'iic', 'sps', 'scr', 'pfilamps', 'dcb', 'dcb2']
            models.extend([f'xcu_{self.piepanName}'])
            models.extend([m % self.ids.idDict for m in ('enu_%(specName)s',)])
            models.extend([f'hx_{self.piepanName}'])

            if self.ids.idDict['site'] == 'J':
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
