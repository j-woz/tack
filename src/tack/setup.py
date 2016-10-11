
# SETUP.PY

import logging
import sys

from tack.Tack import Tack

tack = None

def usage():
    print("usage: tack <file>")

def command(argv):

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y/%m/%d %H:%M:%S')

    if len(argv) != 2:
        usage()
        sys.exit(1)

    tack = Tack(argv[1])
