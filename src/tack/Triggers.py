
# TRIGGERS.PY

import logging
import sys
import time

defaultDefault = object()

class TriggerFactory:
    def __init__(self, tack):
        self.tack = tack
        self.kinds = { "timer"   : TimerTrigger,
                       "process" : ProcessTrigger,
                       "globus"  : GlobusTrigger,
                       "reader"  : ReaderTrigger
                     }

    def new(self, **kwargs):

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

    def __init__(self, tack, args, kind="SUPER"):
        self.constructor(tack, args, kind)

    def constructor(self, tack, args, kind):
        self.tack = tack
        self.id = self.tack.make_id()
        self.kind = kind

        self.name = self.key(args, "name")

        logging.info("New Trigger: %s" % str(self))

    def __str__(self):
        return "%s <%i>" % (self.name, self.id)

    # d: a dictionary ; k: the key ; default: optional default value
    def key(self, d, k, default=defaultDefault):
        try:
            result = d[k]
        except KeyError:
            if default is defaultDefault:
                logging.critical("Given trigger kind=%s with no %s!" %
                                 (self.kind, k))
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
        self.constructor(tack, args, kind="timer")
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
from Queue import Queue, Empty

class ProcessTrigger(Trigger):
    def __init__(self, tack, args):
        self.constructor(tack, args, kind="process")
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
        self.debug("process thread for <%i>: %s" %
                   (self.id, self.command))
        import subprocess
        tokens = self.command.split()
        # cp = subprocess.call(tokens)
        process = subprocess.Popen(tokens)
        self.debug("pid is %i for: %s" % (process.pid, self.command))
        while True:
            p = process.poll()
            if not p is None:
                break
            try:
                message = self.q_down.get(timeout=1)
            except Empty:
                continue
            assert(message == "TERMINATE")
            self.info("terminating pid: %i: %s" %
                      (process.pid, self.command))
            try:
                process.terminate()
            except OSError:
                self.info("process <%i> already exited.")
            process.poll()
            break
        self.debug("run(): done")
        self.q_up.put(process.returncode)

    def shutdown(self):
        # print("subprocess is running: normal shutdown is impossible!")
        self.q_down.put("TERMINATE")
        message = self.q_up.get()
        self.debug("returncode: " + str(message))

class GlobusTrigger(Trigger):
    def __init__(self, tack, args):
        self.constructor(tack, args, kind="globus")
        self.user  = self.key(args, "user")
        self.token = self.key(args, "token")
        self.task  = self.key(args, "task")
        logging.info("New GlobusTrigger \"%s\" <%i> (%s)" %
                     (self.name, self.id, self.task))
        self.handler = self.key(args, "handler")
        self.q = Queue()
        threading.Thread(target=self.run).start()

    def poll(self):
        self.debug("poll()")
        try:
            status = self.q.get_nowait()
        except Empty:
            return
        self.debug("status: " + status)
        self.handler(self, status)
        self.tack.remove(self)

    def run(self):
        self.debug("thread for <%i>: %s" % (self.id, self.task))
        from globusonline.transfer.api_client \
            import TransferAPIClient
        token = self.get_token()
        api = TransferAPIClient(self.user, goauth=token)

        while True:
            code, reason, data = api.task(self.task, fields="status")
            status = data["status"]
            print(status)
            if status in ("SUCCEEDED", "FAILED"):
                break

        self.debug("Globus: done " + status)
        self.q.put(status)

    def get_token(self):
        if self.token == "ENV":
            import os
            v = os.getenv("TOKEN")
            if v == None:
                print("Globus token environment variable TOKEN is unset!")
                sys.exit(1)
            else:
                result = v
        else:
            result = self.token
        return result

class ReaderTrigger(Trigger):

    def __init__(self, tack, args):
        self.constructor(tack, args, kind="reader")
        self.filename = self.key(args, "filename")
        self.eof      = self.key(args, "eof")
        self.pattern  = self.key(args, "pattern", default=None)
        self.eof_obj  = object()

        if self.pattern:
            self.pc = re.compile(self.pattern)
        logging.info("New ReaderTrigger \"%s\" <%i> (%s)" %
                     (self.name, self.id, self.filename))
        self.handler = self.key(args, "handler")
        self.q = Queue()
        threading.Thread(target=self.run).start()

    def poll(self):
        self.debug("poll()")
        try:
            line = self.q.get_nowait()
        except Empty:
            return
        if (not line is self.eof_obj):
            self.debug("line: " + line)
            self.handler(self, line)
        else:
            self.debug("found EOF: " + self.eof)
            self.tack.remove(self)

    def run(self):
        self.debug("thread for <%i>: %s" % (self.id, self.filename))
        with open(self.filename, "r") as f:
            delay_max = 1.0
            delay_min = 0.1
            delay = delay_min
            while True:
                line = f.readline()
                if len(line) == 0:
                    time.sleep(delay)
                    delay = delay_incr(delay, delay_max)
                if (not self.pattern) or self.pc.match(line):
                    self.q.put(line)
                    delay = delay_min
                elif line == self.eof:
                    break
        self.debug("Reader: done: " + filename)
        self.q.put(self.eof_obj)

def delay_incr(delay_now, delay_max):
    if delay_now < 1.0:
        result = delay_now + 0.1
    else:
        result = delay_now + 1.0
        if result > delay_max:
            result = delay_max
    return result
