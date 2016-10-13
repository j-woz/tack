
# TACK.PY

# A context for a Tack instance

import logging
import sys

class Tack:

    def __init__(self, filename):

        self.trigger_id_unique = 1
        self.filename = filename
        # Global user data space
        self.scratch = {}
        # All registered Triggers
        self.triggers = {}
        # Triggers to be removed after poll iterations
        self.removals = []
        self.shutdown_requested = False
        self.interrupted = False

        logging.info("Tack file: " + self.filename)

        self.settings()
        self.start()
        try:
            self.loop()
        except KeyboardInterrupt:
            print(" tack: handling keyboard interrupt")
            self.interrupted = True
        self.shutdown()

    def settings(self):
        self.interval = 1.0

    def start(self):
        with open(self.filename, "r") as f:
            text = f.read()
        exec(text, None, { "tack":self } )

    def make_id(self):
        result = self.trigger_id_unique
        self.trigger_id_unique += 1
        return result

    def loop(self):
        from time import sleep
        while True:
            # Poll all Triggers
            for t in self.triggers.values():
                t.poll()
                if self.shutdown_requested:
                    self.shutdown()
            # Execute all removals
            if len(self.removals) > 0:
                for id in self.removals:
                    del self.triggers[id]
                self.removals = []
            sleep(self.interval)

    def add(self, trigger):
        self.triggers[trigger.id] = trigger

    def remove(self, trigger):
        self.removals.append(trigger.id)

    def request_shutdown(self, trigger):
        logging.info("Shutdown requested by %s" % str(trigger))
        self.shutdown_requested = True

    def shutdown(self):
        for t in self.triggers.values():
            t.shutdown()
        message = "Normal shutdown." if not self.interrupted else \
                  "Normal shutdown after interrupt."
        logging.info(message)
        sys.exit(0)
