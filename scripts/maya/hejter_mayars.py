import sys
import json
import pymel.core as pm


_, assembly_json_file, start_frame, end_frame = sys.argv

with open(assembly_json_file) as json_file:
    json_data = json.load(json_file)
    filename = json_data['filename']
    camera1 = json_data['camera']

pm.openFile(filename,f=True)

pm.mel.eval('redshiftRegisterRenderer(); if( catchQuiet( eval( "redshiftGetRedshiftOptionsNode()" ) ) ) { eval( "redshiftGetRedshiftOptionsNode(true);"); }')
cam1 = pm.PyNode(camera1)
cam1.renderable.set(True)

render_globals = pm.PyNode('defaultRenderGlobals')
render_globals.startFrame.set(int(start_frame))
render_globals.endFrame.set(int(end_frame))


print >> sys.stderr, '#'*20, render_globals.startFrame.get(), render_globals.endFrame.get()
pm.rsRender(render=True, blocking=True, animation=True, cam=cam1)  
