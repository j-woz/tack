
# TRIGGERS.PY

import logging
import sys
import time

defaultDefault = object()

class TriggerFactory:
    def __init__(self, tack):
        self.tack = tack
        self.kinds = { "timer":TimerTrigger, "process":ProcessTrigger }

    def new(self, **kwargs):

        print(kwargs)

        try:
            t = kwargs["kind"]
        except:
            logging.critical("Given trigger with no kind!")
            sys.exit(1)

        if not t in self.kinds:
            logging.critical("No such kind: " + t)
            sys.exit(1)

        T = self.kinds[t]
        result = T(self.tack, kwargs)
        self.tack.add(result)
        return result

class Trigger:

    def __init__(self, tack, args):
        self.tack = tack
        self.id = self.tack.make_id()
        self.kind = "SUPER"

        try:
            self.name = args["name"]
        except KeyError:
            logging.critical("Given trigger with no name!")
            sys.exit(1)

        logging.info("New Trigger: %s" % str(self))

    def __str__(self):
        return "%s <%i>" % (self.name, self.id)

    # d: a dictionary ; k: the key ; default: optional default value
    def key(self, d, k, default=defaultDefault):
        try:
            result = d[k]
        except KeyError:
            if default is defaultDefault:
                logging.critical("Given %s trigger with no %s!" %
                                 (kind, k))
                sys.exit(1)
            else:
                return default
        return result

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
        self.interval = self.key(args, "interval", 0)
        logging.info("New TimerTrigger \"%s\" (%0.3fs)" % \
                     (self.name, self.interval))
        self.last_poll = time.time()
        self.handler = self.key(args, "handler")

    def poll(self):
        self.debug("poll()")
        t = time.time()
        if t - self.last_poll > self.interval:
            self.debug("Calling handler")
            self.handler(self, t)
            last_poll = t

import threading
from queue import Queue, Empty

class ProcessTrigger(Trigger):
    def __init__(self, tack, args):
        super().__init__(tack, args)
        self.command = args["command"]
        logging.info("New ProcessTrigger \"%s\" <%i> (%s)" %
                     (self.name, self.id, self.command))
        self.handler = self.key(args, "handler")
        self.q_down = Queue()
        self.q_up   = Queue()
        threading.Thread(target=self.run).start()

    def poll(self):
        self.debug("poll()")
        try:
            returncode = self.q_up.get_nowait()
        except Empty:
            return
        self.debug("returncode: " + str(returncode))
        self.handler(self, returncode)
        self.tack.remove(self)

    def run(self):
        self.debug("process thread for <%i>: %s" % (self.id, self.command))
        # from time import sleep
        # time.sleep(2)
        import subprocess
        tokens = self.command.split()
        cp = subprocess.run(tokens)
        self.debug("run(): done")
        self.q_up.put(cp.returncode)
