import os
import sys
import tempfile


import copy
from SimpleXMLRPCServer import *
import thread
import socket


class HaStitch(object):
    def add_frame_file(self, filepath, i):
        sys.__stdout__.write( "%s %s\n" % (i, filepath))
    
    def stitch_file(self, msg):
        sys.__stdout__.write( "%s\n" % msg)
    
    def convert_to_abc(self, msg):
        sys.__stdout__.write( "%s\n" % msg)
        print "%s" % msg

    def clean_scratch(self, msg):
        sys.__stdout__.write( "%s\n" % msg)



if __name__ == "__main__":

    IP=sys.args[1]
    PORT==sys.args[2]

    def dispatchForever(x):
        server = SimpleXMLRPCServer((IP, PORT), allow_none=1, logRequests = False)
        sys.__stdout__.write( "Listening on port %s:%s...\n" % (IP,PORT) )
        server.register_instance(HaStitch())
        server.serve_forever()


    thread.start_new_thread(dispatchForever, (None,))