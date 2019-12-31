import os
import sys
import pymel.core as pm
import time
import json
import xmlrpclib
import socket


pm.loadPlugin( 'pxrUsd' )
pm.loadPlugin( 'pxrUsdTranslators' )



import pxr.UsdMaya.userExportedAttributesUI as pea


halembic_json = '/STUDIO/maya/maya2018/scripts/Hamak/halembic.json'


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


# responce = None 
# while responce == None:
#     responce = user_proxy.get_stitch_host()
#     time.sleep(1)
    
# server_ip, server_port = responce

# proxy = xmlrpclib.ServerProxy("http://%s:%s/" % (server_ip, server_port))

CMD="""file -force -options ";\
shadingMode=none;\
materialsScopeName=Looks;\
exportDisplayColor=0;\
exportRefsAsInstanceable=0;\
exportUVs=1;\
exportMaterialCollections=0;\
materialCollectionsPath=Mat;\
exportCollectionBasedBindings=0;\
exportColorSets=0;\
exportReferenceObjects=0;\
renderableOnly=0;\
filterTypes=;\
defaultCameras=0;\
renderLayerMode=defaultLayer;\
mergeTransformAndShape=1;\
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


def render_usd_chank(start_frame, end_frame, filepath, output_directory, set_maya_name, proxy=None, stitch_server=None):

    preset = {}
    if os.path.exists(halembic_json):
        with open(halembic_json,'r') as fin:
            preset = json.load(fin)
    else:
        print "[ HA WARNING ] Export extra attributes skipped. File %s does not exists" % halembic_json

    filename = os.path.basename(filepath)
    print "[ HA MESSAGE ] Try to open file:", filepath
    pm.openFile(filepath, f=True)
    pm.select(cl=True)
    maya_set = pm.PyNode(set_maya_name)
    for item in maya_set.elements():
        print "[ HA MESSAGE ] Select for export: ",item.fullPath()
        pm.select(item.fullPath(), add=True)

        continue

        if 'extra_attributes' in preset:
            extra_attributes = preset['extra_attributes']
            hamak_attrs = []
            for attr in item.listAttr():
                if attr.shortName() in extra_attributes:
                    hamak_attrs += [attr.shortName()]
            if hamak_attrs != []:
                print "[ HA MESSAGE ] Extra attribute found."
                exportedAttrs = [pea.ExportedAttribute(x) for x in hamak_attrs]
                pea.ExportedAttribute.UpdateExportedAttributesForNode(item.fullPath(),exportedAttrs)


    for i in xrange(int(start_frame), int(end_frame)):
        cmds.currentTime(i,edit=True)
        padd=str(i).zfill(6)
        output_filepath = output_directory + os.sep + filename + '.' + padd +'.usd'
        pm.mel.eval(CMD % {'frame':i, 'output_filepath': output_filepath} )

        try:
            proxy.add_frame_file(output_filepath, padd)
        except Exception, e:
            print >> sys.stderr, str(e)

        try:
            stitch_server.add_frame_file(output_filepath, padd)
        except Exception, e:
            print >> sys.stderr, str(e)



if __name__ == "__main__":
    _, proxy_ip, proxy_port, scratch_ma_filepath, scratch_info_filepath, set_maya_name, scratch_usdx_directory, start_frame, end_frame, = sys.argv

    if not os.path.exists(scratch_ma_filepath):
        raise Exception("[  HA ERROR  ] Path not found %s. Failed" % scratch_ma_filepath)

    if not os.path.exists(scratch_usdx_directory):
        raise Exception("[  HA ERROR  ] Path not found %s. Failed" % scratch_usdx_directory)
        
    try:
        proxy = xmlrpclib.ServerProxy("http://%s:%s/" % (proxy_ip, proxy_port))
        with open(temp_sharedserver_infofile,'r') as fout:
            IP, PORT = fout.readline().strip().split(";")
            stitch_server = xmlrpclib.ServerProxy("http://%s:%s/" % (IP, PORT))
    except:
        proxy = None
        stitch_server = None

    render_usd_chank(start_frame, end_frame, scratch_ma_filepath, scratch_usdx_directory, set_maya_name, proxy, stitch_server)

"""
stch=`ls *.usd` && time usdstitch $stch --out gir.usd && time usdcat gir.usd --out gir.abc

real    8m38.994s
user    5m30.059s
sys 6m4.860s

real    2m58.076s
user    2m10.727s
sys 0m19.106s

"""