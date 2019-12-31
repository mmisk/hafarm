import os
import sys
import json
import pymel.core as pm
import contextlib

# LIBQUICKTIME_PLUGIN_DIR

os.environ['MAYA_DISABLE_CIP'] = '1'
import maya.standalone
maya.standalone.initialize()

_, assembly_json_file = sys.argv

with open(assembly_json_file) as json_file:
    json_data = json.load(json_file)
    filename = json_data['filename']
    camera1 = json_data['camera']

pm.openFile(filename,force=True)

shortn = cmds.file(q=1, sn=1, shn=1)[:-3]
rDir = cmds.workspace( q=True, rd=True ) #rootDirectory
filedir = "images/" + os.getlogin() + "/" #'local' save to dir
fDir = rDir + filedir #root direcotry + 'local' file Dir

sceneDir = os.path.dirname(os.path.dirname(filename))
sceneNumber = os.path.basename(sceneDir) #Hejter Scene number (if exists)

if "sc" in sceneNumber: #hejter "sc" name exists
    fullName = fDir + sceneNumber + "_" + shortn #used for text field
else:
    fullName = fDir + shortn #used for text field

fullName = "{0}.mov".format(fullName)

print '[ HA MESSAGE ] Playblast save to', fullName

#SOUND
gPlayBackSlider = maya.mel.eval( '$tmpVar=$gPlayBackSlider' ) #sound checking stuff
snd = cmds.timeControl(gPlayBackSlider, q=True,s=True)

# cmds.lookThru(camera1)
#VERSION
pName = fullName #lineEdit output
pNameVer = pName[-7:-4]
shortn = cmds.file(q=1, sn=1, shn=1)[:-3] #scene short name (update)
fileVer = shortn[-3:] #get file version (update)
qValue = 100


@contextlib.contextmanager
def solo_renderable(solo_cam):

    # Disable all cameras as renderable
    # and store the original states
    cams = cmds.ls(type='camera')
    states = {}
    for cam in cams:
        states[cam] = cmds.getAttr(cam + '.rnd')
        cmds.setAttr(cam + '.rnd', 0)
       
    # Change the solo cam to renderable
    cmds.setAttr(solo_cam + '.rnd', 1)

    try:
        yield
    finally:
        # Revert to original state
        for cam, state in states.items():
            cmds.setAttr(cam + '.rnd', state)

with solo_renderable(camera1):
    cmds.playblast(f=fullName, fmt="qt", c="jpeg", fo=True, qlt=qValue, v=0, s=snd, p=100, wh=[1920, 805], orn=True, os=True)

maya.standalone.uninitialize()

