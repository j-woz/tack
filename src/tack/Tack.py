
# TACK.PY

# A context for a Tack instance

import logging
import sys

class Tack:

    def __init__(self, filename):

        self.trigger_id_unique = 1
        self.filename = filename
        self.scratch = {}
        self.triggers = {}
        self.shutdown_requested = False

        logging.info("Tack file: " + self.filename)

        self.settings()
        self.start()
        self.loop()
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
            for t in self.triggers.values():
                t.poll()
                if self.shutdown_requested:
                    self.shutdown()
            sleep(self.interval)

    def add_trigger(self, trigger):
        self.triggers[trigger.id] = trigger

    def request_shutdown(self, trigger):
        logging.info("Shutdown requested by %s" % str(trigger))
        self.shutdown_requested = True

    def shutdown(self):
        for t in self.triggers.values():
            t.shutdown()
        logging.info("Normal shutdown.")
        sys.exit(0)
