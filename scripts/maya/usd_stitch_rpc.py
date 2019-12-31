import os
import sys
import glob
import xmlrpclib
import thread
import socket
from pxr import UsdUtils, Sdf, Tf
from SimpleXMLRPCServer import SimpleXMLRPCServer
_, SERVER_IP, SERVER_PORT, scratch_file_basename, start_frame, end_frame, temp_sharedserver_infofile  = sys.argv

output_directory = '/tmp/' + scratch_file_basename


def findFreePort(hostname, port):
    """
    Find a port that's not taken
    Note: when port == 0, we are asking the system to grab
    a random free port.  Otherwise we are searching for one
    from the given range
    """
    for n in range(port,65535):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_addr = (hostname, n)
            s.bind(test_addr)
            # it worked, lets go with that
            found_port = s.getsockname()[1]
            s.close()
            return found_port
        except socket.error as e:
            if e.errno == 98 or e.errno == 10048:
                #logger.debug("Port {} is already in use".format(n))
                continue
            else:
                raise
    raise RuntimeError("Could not find a free TCP in range {}-65535".format(port))


s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
IP = s.getsockname()[0]
PORT = findFreePort(IP,30000)


with open(temp_sharedserver_infofile, 'w') as fout:
    fout.write("%s;%s"%(IP,PORT))

try:
    proxy = xmlrpclib.ServerProxy("http://%s:%s/"%(SERVER_IP,SERVER_PORT))
except:
    pass    


def shutdown_thread():
    server.shutdown()



class HaStitch(object):
    _start_frame=0
    _end_frame=0
    _file_name=None
    _files_to_stitch={}
    _outLayer = None
    _counter = []

    def __init__(self, start_frame, end_frame):
        self._out_file = output_directory + os.sep +'out.usdc'
        self._outLayer = Sdf.Layer.CreateNew(self._out_file)
        self._counter = list(xrange(start_frame, end_frame))


    def add_frame_file(self, filepath, i):
        print i, self._counter
        self._files_to_stitch[i] = Sdf.Layer.FindOrOpen(filepath)
        print self._files_to_stitch[i] == None
        for n in self._files_to_stitch.keys():
            j = min(self._counter)
            print "##########", j
            to_stitch = self._files_to_stitch.get(j)
            if to_stitch != None:
                UsdUtils.StitchLayers(self._outLayer, to_stitch)
                self._counter.remove(n)
                del self._files_to_stitch[n]

        print len(self._counter), i, filepath
        print 
        if self._counter == []:
            self._outLayer.Save()
            print "[HA MESSAGE] DONE", self._out_file
            thread.start_new_thread(shutdown_thread, ())

        return True


server = SimpleXMLRPCServer((IP, PORT), allow_none=1, logRequests = False)
sys.__stdout__.write( "Listening on port %s:%s...\n" % (IP,PORT) )
server.register_instance(HaStitch(int(start_frame), int(end_frame)))
server.serve_forever()


# try:
#     proxy.stitch_file("[ HA MESSAGE ] DONE USD: %s/out.usdc" % output_directory)
# except Exception, e:
#     pass




