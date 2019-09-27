import os
import sys
import pymel.core as pm
import time
import xmlrpclib
import socket

pm.loadPlugin( 'pxrUsd' )
pm.loadPlugin( 'pxrUsdTranslators' )

if len(sys.argv) != 7:
    print >> sys.stderr, "Wrong length number of arguments. Exit..."
    exit(0)

_, start_frame, end_frame, filepath, target, proxy_ip, proxy_port = sys.argv


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

try:
    proxy = xmlrpclib.ServerProxy("http://%s:%s/" % (proxy_ip, proxy_port))
except:
    pass

# responce = None 
# while responce == None:
#     responce = user_proxy.get_stitch_host()
#     time.sleep(1)
    
# server_ip, server_port = responce

# proxy = xmlrpclib.ServerProxy("http://%s:%s/" % (server_ip, server_port))

filename = os.path.basename(filepath)

output_directory = '/SCRATCH/temp/' + filename

if not os.path.exists(output_directory):
    os.makedirs(output_directory)

pm.openFile(filepath, f=True)
pm.select(cl=True)

for n in target.split(','):
    pm.select(n, add=True)


cmd="""file -force -options ";\
shadingMode=useRegistry;\
materialsScopeName=Looks;\
exportDisplayColor=0;\
exportRefsAsInstanceable=0;\
exportUVs=0;\
exportMaterialCollections=0;\
materialCollectionsPath=Mat;\
exportCollectionBasedBindings=0;\
exportColorSets=0;\
exportReferenceObjects=0;\
renderableOnly=0;\
filterTypes=;\
defaultCameras=0;\
renderLayerMode=defaultLayer;\
mergeTransformAndShape=0;\
exportInstances=0;\
defaultMeshScheme=catmullClark;\
exportSkels=none;\
exportSkin=none;\
exportVisibility=0;\
stripNamespaces=1;\
animation=1;\
eulerFilter=0;\
startTime=%(frame)s;\
endTime=%(frame)s;\
frameStride=1;\
parentScope=;\
compatibility=none" -typ "pxrUsdExport" -pr -es \
"%(output_filepath)s";"""


for i in xrange(int(start_frame), int(end_frame)):
    cmds.currentTime(i,edit=True)
    output_filepath = output_directory + os.sep + filename + '.' + str(i).zfill(6) +'.usd'
    pm.mel.eval(cmd % {'frame':i, 'output_filepath': output_filepath} )
    try:
        proxy.add_frame_file(output_filepath, i)
    except Exception, e:
        print >> sys.stderr, str(e)
"""
stch=`ls *.usd` && time usdstitch $stch --out gir.usd && time usdcat gir.usd --out gir.abc

real    8m38.994s
user    5m30.059s
sys 6m4.860s

real    2m58.076s
user    2m10.727s
sys 0m19.106s

"""