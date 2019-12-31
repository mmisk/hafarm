import os
import sys
import hou

USD_ABC_HIP=os.path.expandvars('$REZ_HAFARM_ROOT/py/hafarm/scripts/houdini/usb2abc_converter_v02.hip')
print USD_ABC_HIP
usd_file = sys.argv[1]
abc_file = sys.argv[2]
remove_path = sys.argv[3]
start_frame = sys.argv[4]
end_frame = sys.argv[5]


hou.hipFile.load(USD_ABC_HIP)

n = hou.node('/obj/usd_convert_v01/file2')
n.parm("import_file").set(usd_file)

a = hou.node('/obj/usd_convert_v01/rop_alembic1')
a.parm('filename').set(abc_file)
a.parm("f1").set(int(start_frame))
a.parm("f2").set(int(end_frame))
a.parm('execute').pressButton()
