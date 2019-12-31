import os
import sys
import glob
import xmlrpclib
import thread
import socket
import subprocess
from pxr import UsdUtils, Sdf, Tf
from SimpleXMLRPCServer import SimpleXMLRPCServer


#python /opt/rez_packages/hafarm/v0.3.3/py/hafarm/scripts/maya/usd2abc.py 
#10.20.1.121 30000 1001 1210 /SCRATCH/temp/snazarenko_sh0030_ANM_v014.ma_5ZVE_Vprasr/body/references /SCRATCH/temp/snazarenko_sh0030_ANM_v014.ma_5ZVE_Vprasr/body/out.usdc 
_, SERVER_IP, SERVER_PORT, start_frame, end_frame, abc_filepath, scratch_usdc_filepath = sys.argv

try:
    proxy = xmlrpclib.ServerProxy("http://%s:%s/"%(SERVER_IP,SERVER_PORT))
except:
    pass


USD_ABC_PY=os.path.expandvars('$REZ_HAFARM_ROOT/py/hafarm/scripts/houdini/usb2abc_converter.py')

cmd2 = 'rez env houdini-17.5 gcc -- hython %s %s %s "/RIG/geometry_hrc" %s %s' % (USD_ABC_PY, scratch_usdc_filepath, abc_filepath, start_frame, end_frame)
# cmd2 = 'usdcat  %s/out.usdc --out %s' % ( output_directory, out_filepath )
print cmd2
exit_code = subprocess.call(cmd2, shell=True, stderr=subprocess.STDOUT )


try:
    proxy.convert_to_abc("[ HA MESSAGE ] DONE ABC: %s " % abc_filepath)
except:
    pass


# if os.path.exists(out_filepath):
#     for n in os.listdir(output_directory):
#         os.remove(output_directory + os.sep + n)
#     try:
#         proxy.clean_scratch("[ HA_MESSAGE ] Clean scratch %s" % output_directory)
#     except:
#         pass

sys.exit(exit_code)
