#!/usr/bin/env python

import actorcore.ICC

class OurActor(actorcore.ICC.ICC):
    def __init__(self, name,
                 productName=None, configFile=None,
                 modelNames=('hx'),
                 debugLevel=30):

        """ Setup an Actor instance. See help for actorcore.Actor for details. """
        
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        actorcore.ICC.ICC.__init__(self, name, 
                                   productName=productName, 
                                   configFile=configFile,
                                   modelNames=modelNames)

        self.everConnected = False
        
    def connectionMade(self):
        if self.everConnected is False:
            self.logger.info("Attaching all controllers...")
            self.allControllers = [s.strip() for s in self.config.get(self.name, 'startingControllers').split(',')]
            self.attachAllControllers()
            self.everConnected = True
        

#
# To work
def main():
    theActor = OurActor('hx', productName='hxActor')
    theActor.run()

if __name__ == '__main__':
    main()
