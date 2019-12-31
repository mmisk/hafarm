import os
import sys
import glob
import xmlrpclib
import thread
import socket
from pxr import UsdUtils, Sdf, Tf
from SimpleXMLRPCServer import SimpleXMLRPCServer
_, SERVER_IP, SERVER_PORT, scratch_usdx_directory, scratch_usdc_filepath, _ = sys.argv

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

# s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# s.connect(("8.8.8.8", 80))
# IP = s.getsockname()[0]
# PORT = findFreePort(IP,30000)
try:
    proxy = xmlrpclib.ServerProxy("http://%s:%s/"%(SERVER_IP,SERVER_PORT))
except:
    pass    

# start_frame, end_frame = proxy.get_info()

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
        self._out_file = '/tmp/koko.usd'
        self._outLayer = Sdf.Layer.CreateNew(self._out_file)
        self._counter = list(xrange(start_frame, end_frame))

    def set_start_frame(self, val):
        self._start_frame = val

    def set_end_frame(self, val):
        self._end_frame = val

    def set_file_name(self, val):
        self._file_name = val

    def add_frame_file(self, filepath, i):
        self._files_to_stitch[i] = Sdf.Layer.FindOrOpen(filepath)
        for n in self._files_to_stitch.keys():
            if to_stitch != None:
                to_stitch = self._files_to_stitch.get(min(self._counter))
                self._counter.remove(n)
            # UsdUtils.StitchLayers(outLayer, to_stitch)
        print len(self._counter), filepath
        if self._counter == []:
            self._outLayer.Save()
            print "try to exit"
            thread.start_new_thread(shutdown_thread, ())

        return True


list_usd_files = glob.glob('%s/*.usd'%scratch_usdx_directory)
list_usd_files.sort()
print "#"*20, scratch_usdx_directory
import subprocess


cmd1 = 'usdstitch %s -o %s' % ( ' '.join(list_usd_files), scratch_usdc_filepath)
print cmd1
exit_code = subprocess.call(cmd1, shell=True, stderr=subprocess.STDOUT )


try:
    proxy.stitch_file("[ HA MESSAGE ] DONE USD: %s" % scratch_usdc_filepath)
except Exception, e:
    pass

exit(exit_code)


