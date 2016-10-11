
# TRIGGERS.PY

import logging
import sys

class TriggerFactory:
    def __init__(self, tack):
        self.tack = tack
        self.types = { "timer":TimerTrigger }

    def new(self, **kwargs):

        print(kwargs)

        try:
            t = kwargs["type"]
        except:
            logging.critical("Given trigger with no type!")
            sys.exit(1)

        if not t in self.types:
            logging.critical("No such type: " + t)
            sys.exit(1)

        T = self.types[t]
        result = T(self.tack, kwargs)
        self.tack.add_trigger(result)
        return result

class Trigger:

    def __init__(self, tack, args):
        self.tack = tack
        self.id = self.tack.make_id()

        try:
            self.name = args["name"]
        except KeyError:
            logging.critical("Given trigger with no name!")
            sys.exit(1)

        logging.info("New Trigger: %s" % str(self))

    def __str__(self):
        return "%s <%i>" % (self.name, self.id)

    def poll(self):
        logging.info("Default poll(): %s" % str(self))

class TimerTrigger(Trigger):
    def __init__(self, tack, args):
        super().__init__(tack, args)
        logging.info("New TimerTrigger")
