import sys
import pymel.core as pm

if len(sys.argv) != 5:
    print >> sys.stderr, "Wrong length number of arguments. Exit..."
    exit(0)

_, start_frame, end_frame, filename, camera1 = sys.argv


pm.openFile(filename,f=True)

pm.mel.eval('redshiftRegisterRenderer(); if( catchQuiet( eval( "redshiftGetRedshiftOptionsNode()" ) ) ) { eval( "redshiftGetRedshiftOptionsNode(true);"); }')
# pm.workspace(fileRule=['images', "/PROD/dev/sandbox/user/snazarenko/software/maya/images/snazarenko/"])
cam1 = pm.PyNode(camera1)
cam1.renderable.set(True)

render_globals = pm.PyNode('defaultRenderGlobals')
render_globals.startFrame.set(int(start_frame))
render_globals.endFrame.set(int(end_frame)-1)


print >> sys.stderr, '#'*20, render_globals.startFrame.get(), render_globals.endFrame.get()
pm.rsRender(render=True, blocking=True, animation=True, cam=cam1)  
