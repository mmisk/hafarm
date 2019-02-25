#!/usr/bin/python
import mantra
import random
from optparse import OptionParser
import sys, logging

DENOISE = False

def parseOptions():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)

    parser.add_option("", "--denoise", dest="denoise",  default="True", action="store_true", 
        help="Turn off dorayvariance propertty and set random seed to true random (same frame will differ rendered twice.)")
    (opts, args) = parser.parse_args(sys.argv[1:])
    return opts, args

def filterInstance():
    logger = logging.getLogger('filterInstance')
    if DENOISE:
        mantra.setproperty("object:dorayvariance", 0)
        logger.info("{}:dorayvariance => {}".format(mantra.property('object:name')[0], 0))

def filterRender():
    logger = logging.getLogger('filterRender')
    if DENOISE:
        seed = random.randint(1,1e6)
        mantra.setproperty("image:randomseed", seed)
        logger.info("image:randomseed => {}".format(seed))
        



def main():

    opts, args = parseOptions()
    logging.basicConfig(format='%(asctime)s, %(name)s - %(levelname)s: %(message)s',  
        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
    logger = logging.getLogger('mantraRender4Altus.py')

    if opts.denoise:
        global DENOISE
        DENOISE = True


if __name__ == "__main__": main()