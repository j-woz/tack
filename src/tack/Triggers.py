
# TRIGGERS.PY

import logging
import sys
import time

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

    def info(self, message):
        logging.info("%s: %s" % (str(self), message))

    def debug(self, message):
        logging.debug("%s: %s" % (str(self), message))

    def poll(self):
        logging.info("Default poll(): %s" % str(self))

    def request_shutdown(self):
        self.tack.request_shutdown(self)

    def shutdown(self):
        logging.info("Default shutdown(): %s" % str(self))


class TimerTrigger(Trigger):

    def __init__(self, tack, args):
        super().__init__(tack, args)
        self.interval = args["interval"]
        logging.info("New TimerTrigger \"%s\" (%0.3fs)" %
                     (self.name, self.interval))
        self.last_poll = time.time()
        try:
            self.handler = args["handler"]
        except KeyError:
            logging.critical("Given timer trigger with no handler!")
            sys.exit(1)

    def poll(self):
        self.debug("poll()")
        t = time.time()
        if t - self.last_poll > self.interval:
            self.debug("Calling handler")
            self.handler(self, t)
            last_poll = t
