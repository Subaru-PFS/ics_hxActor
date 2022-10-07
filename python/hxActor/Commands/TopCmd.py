#!/usr/bin/env python

import logging

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

class TopCmd(object):

    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('ping', '', self.ping),
            ('status', '', self.status),
            ('setLogger', '<log> <level>', self.setLogger),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("mcs_mcs", (1, 1),
                                        keys.Key("log", types.String(), default=None,
                                                 help='the logger name'),
                                        keys.Key("level", types.Int(), default=None,
                                                 help='the logger level'),
                                        )


    def ping(self, cmd):
        """Query the actor for liveness/happiness."""

        cmd.warn("text='I am an empty and fake actor'")
        cmd.finish("text='Present and (probably) well'")

    def status(self, cmd):
        """Report camera status and actor version. """

        self.actor.sendVersionKey(cmd)
        
        cmd.inform('text="Present!"')
        cmd.finish()

    def setLogger(self, cmd):
        """Query the actor for liveness/happiness."""

        cmdKeys = cmd.cmd.keywords

        log = cmdKeys['log'].values[0]
        level = cmdKeys['level'].values[0]

        try:
            logger = logging.getLogger(log)
        except Exception as e:
            cmd.fail(f'text="no logger named {log}: {e}"')
            return

        logger.setLevel(level)
        cmd.finish()
