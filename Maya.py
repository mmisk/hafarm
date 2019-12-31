import os
import sys
from uuid import uuid4

from PySide2 import QtCore
from PySide2 import QtGui
from PySide2 import QtWidgets
from PySide2 import __version__
from shiboken2 import wrapInstance 
from maya import OpenMayaUI as omui 
import pymel.core as pm
import tempfile
import json


from HaGraph import HaGraph
from HaGraph import HaGraphItem
from HaGraph import random_hash_string
from hafarm import SlurmRender
from hafarm import JsonRigObject

import const
import copy
from SimpleXMLRPCServer import *
import thread
import socket
import re

SCRATCH = os.environ.get('HA_SCRATCH','/mnt/lustre/temp')
pat = re.compile("_v([0-9]+)")

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


HA_MESSENGER = os.environ.get('HA_MESSENGER')
if HA_MESSENGER != None:
    IP,PORT=HA_MESSENGER.split(':')


# startSharedServer(["python", "/opt/package/rez_packages/hafarm/v0.3.3/py/hafarm/scripts/maya/shared_server.py",IP,PORT])



def get_line(filename):
    file = open(filename)
    if (file.read(1).encode("hex") == '2f') == False:
        raise StopIteration
    file.seek(0)
    while True:
        line = file.readline()
        if not line:
            file.close()
            break
        yield line



def save_to_scratch():
    scene_name = pm.sceneName()
    basename = os.path.basename(scene_name)
    scratch_file = tempfile.NamedTemporaryFile(suffix=basename, dir=SCRATCH,delete=False)
    scratch_file.close()
    pm.renameFile(scratch_file.name)
    pm.saveFile()
    pm.renameFile(scene_name)
    os.chmod(scratch_file.name, 0o0777)
    return scratch_file.name



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



# http://openendedgroup.com/field/MayaIntegration.html
def dispatchForever(x):
    server = SimpleXMLRPCServer((IP, PORT), allow_none=1, logRequests = False)
    sys.__stdout__.write( "Listening on port %s:%s...\n" % (IP,PORT) )
    server.register_instance(HaStitch())
    server.serve_forever()


if HA_MESSENGER == None:
    thread.start_new_thread(dispatchForever, (None,))


class HaRedshiftRenderWrapper(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/maya/redshift', '')
        dependencies = kwargs.get('deps',[])
        super(HaRedshiftRenderWrapper, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/mayars.py']
        self.parms['command'] << '{exe} {command_arg} {scene_file} {target_list}'
        script_name = pm.sceneName()
        path, name = os.path.split(script_name)
        self.parms['scene_file'] << { 'scene_fullpath': kwargs.get('scene_file', script_name) }
        self.parms['job_name'] << { "job_basename": name
                                    , "jobname_hash": self.get_jobname_hash()
                                    , "render_driver_name": "redshift"}
        self.parms['req_license'] = 'redshift_lic=1' # maya_lic=1,
        self.parms['step_frame'] = 5
        self.parms['ignore_check'] = True
        self.parms['queue'] = kwargs['queue']
        self.parms['group'] = kwargs['group']
        self.parms['start_frame'] = kwargs['start_frame']
        self.parms['end_frame'] = kwargs['end_frame']
        self.parms['frame_range_arg'] = [' %s %s','start_frame','end_frame']
        self.parms['exclude_list'] = kwargs['exclude_list']
        # self.parms['output_picture'] = kwargs['output_picture']
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['priority'] = kwargs['priority']
        self.parms['target_list'] = kwargs['target_list']



class REFreplace(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/usd/ref_replace', '')
        dependencies = kwargs.get('deps',[])
        super(REFreplace, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)
        
        script_name = pm.sceneName()
        path, name = os.path.split(script_name)
        basename, ext = os.path.splitext(name)

        self.parms['scene_file'] << { "scene_file_path": path
                                        ,"scene_file_basename": basename
                                        ,"scene_file_ext": ext }
        self.parms['job_name'] << { "job_basename": kwargs['jobname']
                                    , "render_driver_name": "ref" }
        self.tags = tags
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['req_resources'] = ''
        self.parms['slots'] = 1
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1
        self.parms['job_wait_dependency_entire'] = True
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        
        hamak_set_maya_name = kwargs['hamak_set_maya_name']
        scenefilepath = kwargs['scenefilepath']
        scratch_outfilename = kwargs['scratch_ma_filepath']
        
        self.parms['command'] << '{exe} {command_arg}' 
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/reference_hires.py', str(IP), str(PORT), hamak_set_maya_name, scenefilepath, scratch_outfilename ] 
        self.parms['job_name'] << { "job_basename": kwargs['jobname'], "render_driver_name": "ref"}



class USDGenerate(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/maya/export_geo', '')
        dependencies = kwargs.get('deps',[])
        super(USDGenerate, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)

        self.parms['job_wait_dependency_entire'] = True
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/render_usd_frames.py %s %s ' % ( IP, PORT)
                                            , kwargs['scratch_ma_filepath']
                                            , kwargs['scratch_info_filepath']
                                            , kwargs['hamak_set_maya_exportname'] 
                                            , kwargs['scratch_usdx_directory'] ]
        self.parms['command'] << ( '{exe} {command_arg} ')
        self.parms['queue'] = kwargs['queue']
        self.parms['group'] = kwargs['group']
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['start_frame'] = kwargs['start_frame']
        self.parms['end_frame'] = kwargs['end_frame']
        self.parms['step_frame'] = kwargs.get('usd_step_frame', 20)
        self.parms['frame_range_arg'] = [' %s %s','start_frame','end_frame']
        self.parms['job_name'] << { "job_basename": kwargs['jobname'], "render_driver_name": "usd"}
        self.parms['target_list'] = kwargs['target_list']


    def copy_scene_file(self, **kwargs):
        pass



class USDstitch(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/usd/stitch', '')
        dependencies = kwargs.get('deps',[])
        super(USDstitch, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)

        self.tags = tags
        self.parms['exe'] = 'rez env usd -- python'
        self.parms['job_wait_dependency_entire'] = True
        self.parms['req_resources'] = ''
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['slots'] = 1
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/usd_stitch.py %s %s ' %( str(IP), str(PORT) ) 
                                                                , kwargs['scratch_usdx_directory']
                                                                , kwargs['scratch_usdc_filepath']
                                                                , kwargs['scratch_info_filepath'] ] #
        self.parms['command'] << '{exe} {command_arg}' 
        self.parms['job_name'] << { "job_basename": kwargs['jobname'], "render_driver_name": "stitch"}


    def copy_scene_file(self, **kwargs):
        pass



class ABCconvert(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/usd/abc', '')
        dependencies = kwargs.get('deps',[])
        super(ABCconvert, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)
        
        script_name = kwargs.get('scenefilepath', pm.sceneName())
        path, name = os.path.split(script_name)
        basename, ext = os.path.splitext(name)

        self.tags = tags
        self.parms['job_wait_dependency_entire'] = True
        self.parms['exe'] = 'rez env usd -- python'
        self.parms['req_resources'] = ''
        self.parms['req_license'] = 'hbatch_lic=1'
        self.parms['slots'] = 1
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/usd2abc.py %s %s %s %s' %( str(IP), str(PORT) , kwargs['start_frame'], kwargs['end_frame'] )
                                                            , kwargs['abc_output_filename']
                                                            , kwargs['scratch_usdc_filepath'] ] #
        self.parms['command'] << '{exe} {command_arg}' 
        self.parms['job_name'] << { "job_basename": kwargs['jobname'], "render_driver_name": "abc"}
    
    def copy_scene_file(self, **kwargs):
        pass



class AssemblyJson(object):
    version = [1,0,0]

    def __init__(self):
        self.scene = {}
        self.resolutions = {}
        self.env = {}
        self.anim_file_version = None
        script_name = pm.sceneName()
        grps = pat.search(script_name)
        if grps:
            self.anim_file_version = grps.groups()[0]
        self.frames = dict( start = int( pm.playbackOptions(q=True,min=True) )
                                    ,end = int( pm.playbackOptions(q=True,max=True) ) )
        self.env['shot'] = os.environ.get('JOB_ASSET_NAME','Unknown')
        self.env['scene'] = os.environ.get('JOB_ASSET_TYPE','Unknown')
        self.env['project'] = os.environ.get('JOB_CURRENT','Unknown')

    def add_scene_item(self, namespace, name, filepath):
        self.scene[namespace] = self.scene.get(namespace, {})
        self.scene[namespace].update( { name: filepath } )

    def add_resolution(self, res, name, data):
        self.resolutions[res] = self.resolutions.get(res, {})
        self.resolutions[res].update({name: data})


    def to_dict(self):
        ret = dict(
             api=self.version
            ,frames=self.frames
            ,version=self.anim_file_version
            ,scene=self.scene
            ,env=self.env
            ,resolutions=self.resolutions
        )
        return ret



class ANMassembly(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/usd/anm', '')
        dependencies = kwargs.get('deps',[])
        super(ANMassembly, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)

        self.parms['job_wait_dependency_entire'] = True
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/hejter_scene_assemble.py', kwargs['assembly_json_file'] ]
        self.parms['command'] << ( '{exe} {command_arg} ')
        self.parms['queue'] = kwargs['queue']
        self.parms['group'] = kwargs['group']
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['job_name'] << { "job_basename": kwargs['jobname'], "render_driver_name": "assembly"}


    def copy_scene_file(self, **kwargs):
        pass



class ASPlayBlast(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/usd/playblast', '')
        dependencies = kwargs.get('deps',[])
        super(ASPlayBlast, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)

        self.parms['job_wait_dependency_entire'] = True
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/hejter_scene_playblast.py', kwargs['assembly_json_file'] ]
        self.parms['command'] << ( '{exe} {command_arg} ')
        self.parms['queue'] = kwargs['queue']
        self.parms['group'] = kwargs['group']
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['job_name'] << { "job_basename": kwargs['jobname'], "render_driver_name": "playblast"}


    def copy_scene_file(self, **kwargs):
        pass



class RSRender(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/maya/rs', '')
        dependencies = kwargs.get('deps',[])
        super(RSRender, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/hejter_mayars.py', kwargs['assembly_json_file']]
        self.parms['command'] << '{exe} {command_arg}'
        script_name = pm.sceneName()
        path, name = os.path.split(script_name)
        self.parms['job_name'] << { "job_basename": name
                                    , "render_driver_name": "redshift"}
        self.parms['req_license'] = 'redshift_lic=1'
        self.parms['step_frame'] = 5
        self.parms['ignore_check'] = True
        self.parms['queue'] = 'cuda'
        self.parms['group'] = kwargs['group']
        self.parms['start_frame'] = kwargs['start_frame']
        self.parms['job_wait_dependency_entire'] = True
        self.parms['end_frame'] = kwargs['end_frame']
        self.parms['frame_range_arg'] = [' %s %s','start_frame','end_frame']
        # self.parms['output_picture'] = kwargs['output_picture']
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['priority'] = kwargs['priority']


    def copy_scene_file(self, **kwargs):
        pass



class HaContextMaya(object):
    def __init__(self, **kwargs):
        script_name = pm.sceneName()

        _, ext = os.path.splitext(script_name)
        if ext == '.mb':
            pm.confirmDialog(message="There is no option to export on render farm the *.mb file. Please, save file as Maya ASCII and try again.", button=["ok"])
            raise Exception("File must be MAYA ASCII")

        self._assembly = kwargs.get('assembly',False)
        if self._assembly == True:
            if pm.renderer( 'redshift', exists=True ) == 0:
                pm.confirmDialog(message="Render shaded option uses Redshift renderer. Please, turn on redshift renderer and try again.", button=["ok"])
                raise Exception("Turn on REDSHIFT")

        if int( pm.playbackOptions(q=True,min=True) ) < 0:
            pm.confirmDialog(message="Negative frames are not supported. Sorry...", button=["ok"])
            raise Exception("Negative frames are not supported")

        self._errors = {}
        self._exportables = kwargs.get('export_extensions',[])
        self._json_rig = None
        self._usd_step_frame = kwargs.get('usd_step_frame',20)
        self._nodes = []
        self._graph = HaGraph(graph_items_args=[])
        self._graph.set_render( kwargs.get('render', SlurmRender.SlurmRender) )
        self._jobname_hash = random_hash_string()
        script_name = pm.sceneName()
        _, name = os.path.split(script_name)
        self.scene_name = name
        self._export_sets = {}
        self.basename, ext = os.path.splitext(name)
        self.tempdir = tempfile.mkdtemp(prefix="_".join([os.getlogin(), self.scene_name, self._jobname_hash+"_"]),dir=SCRATCH)
        os.chmod(self.tempdir, 0o0777)
        self.assembly_json = AssemblyJson()
        self.global_params = dict(
                                     queue = '3d'
                                    ,group = 'allhosts'
                                    ,start_frame = int( pm.playbackOptions(q=True,min=True) )
                                    ,end_frame = int( pm.playbackOptions(q=True,max=True) )
                                    ,job_on_hold = kwargs.get('job_on_hold', False)
                                    ,priority = -500
                                    ,jobname = self.scene_name
                                    ,exclude_list = []
                                    ,usd_step_frame = self._usd_step_frame
                                    ,assembly_json_file = self.tempdir + os.sep + "assembly.json"
                                )
    def __enter__(self):
        self.global_params['scenefilepath'] = save_to_scratch()
        return self


    def __exit__(self, type, value, traceback):
        if self._errors != {}:
            _err = ""
            for n,m in self._errors.iteritems():
                _err += n + ":\n"
                _err += m + "\n\n"
            pm.confirmDialog(message=_err, button=["ok"],title="Error!")
            raise Exception(str(self._errors))


        with open( self.global_params['assembly_json_file'], "w") as json_file:
                json.dump(self.assembly_json.to_dict(), json_file, indent=4)
        print "[ HA MESSAGE ] Assembly json", self.global_params['assembly_json_file']

        if self._assembly == True:
            global_params = copy.copy(self.global_params)
            assembly = ANMassembly(**global_params)
            for n in self._graph.graph_items:
                assembly.add(n)
            self.add_node(assembly)

            # pbnode = ASPlayBlast(**global_params)
            # pbnode.add(assembly)
            # self.add_node(pbnode)

            rsnode = RSRender(**global_params)
            rsnode.add(assembly)
            self.add_node(rsnode)

        self._graph.render()


    def is_shortname_exportable(self, short_name, reference_path):
        # TODO: refactor this
        _json_rig = JsonRigObject.JsonRigObject.create(reference_path)
        _exportables = copy.copy(self._exportables)
        for ext in _exportables:
            _ext_sets = _json_rig.export_sets(ext)
            try:
                self.assembly_json.add_resolution(ext,_json_rig.asset_name(),_ext_sets)
            except:
                pass
            self._export_sets.update( _ext_sets )
            print '[  HA DEBUG  ]', self._export_sets

        try:
            for _res, _path in _json_rig:
                if not os.path.exists(_path):
                    self._errors['FILE NOT FOUND'] = self._errors.get('FILE NOT FOUND','') + _path + "\n"
        except:
            pass
                

        if _json_rig.force == True:
            return True
        try:
            return _json_rig.resolution(short_name) in _exportables
        except:
            pass

        try:
            return _json_rig.extension(short_name) in _exportables
        except:
            pass
        raise Exception('[  HA ERROR  ] Failed run API 1.0.0 and 1.1.0')



    def add_geometry_data(self, target, file_path, *args, **kwargs):
        if target == None:
            return
        namespace = kwargs.get('namespace','')
        self.assembly_json.add_scene_item(namespace,target,file_path)

        tempdir = self.tempdir + os.sep + namespace + os.sep + target
        if not os.path.exists(tempdir):
            os.makedirs(tempdir)
            os.chmod(tempdir, 0o0777)
        if not os.path.exists(tempdir + os.sep + "usdx"):
            os.makedirs(tempdir + os.sep + "usdx")
            os.chmod(tempdir + os.sep + "usdx", 0o0777)

        jobname, _ = os.path.splitext(os.path.basename(file_path))
        global_params = copy.copy(self.global_params)
        
        _global_params = dict(
             target_list = [ target ]
            ,jobname = jobname
            ,hamak_set_maya_name = target
            ,scratch_info_filepath = tempdir + os.sep + 'sharedserver.info'
            ,scratch_ma_filepath = tempdir + os.sep + self.scene_name
            ,scratch_usdx_directory = tempdir + os.sep + "usdx"
            ,scratch_usdc_filepath = tempdir + os.sep + "out.usdc"
            ,generate_assembly_json = tempdir + os.sep + 'assembly.json'
            ,abc_output_filename = file_path
            ,hamak_set_maya_exportname = '"%s:%s"' % ( namespace, self._export_sets[target] )
        )

        global_params.update(_global_params)

        ref = REFreplace(**global_params)
        self.add_node(ref)

        mayanode = USDGenerate(**global_params)
        mayanode.add(ref)
        self.add_node(mayanode)

        usdfarm = USDstitch(**global_params)
        usdfarm.add(mayanode)
        self.add_node(usdfarm)

        abcnode = ABCconvert(**global_params)
        abcnode.add(usdfarm)
        self.add_node(abcnode)


    def add_node(self, node):
        node.parms['job_name'] << { "jobname_hash": self._jobname_hash }
        self._graph.add_node(node)



def maya_main_window():
    main_window_ptr = omui.MQtUtil.mainWindow()
    return wrapInstance(long(main_window_ptr), QtWidgets.QWidget)



class MayaFarmGUI(QtWidgets.QDialog):
    def __init__(self, parent=maya_main_window()):
        super(MayaFarmGUI, self).__init__(parent)
        self.qtSignal = QtCore.Signal()
        #################################################################

    def create_render_panel(self):
        self.setWindowTitle("Hafarm")
        self.setWindowFlags(QtCore.Qt.Tool)
        self.resize(300, 90) # re-size the window
        self.mainLayout = QtWidgets.QVBoxLayout(self)
        
        self.jointTitle = QtWidgets.QLabel(" Redshift render ")         
        self.jointTitle.setStyleSheet("color: white;background-color: black;")
        self.jointTitle.setFixedSize(300,30)
        # self.jointList = QtWidgets.QListWidget(self)
        # self.jointList.resize(300, 300)
        # for i in range(10):
        #     self.jointList.addItem('Item %s' % (i + 1))

        self.cameraLabel1 = QtWidgets.QLabel("Select renderable camera:");
        self.renderable_cameras = QtWidgets.QComboBox()
        self.renderable_cameras.insertItems(0, [str(x.getParent()) for x in pm.ls(cameras=True)  if x.visibility.get() == True] ) #if x.renderable.get() == True])

        # self.skinTitle = QtWidgets.QLabel(" Current Skin Weights")
        # self.skinTitle.setStyleSheet("color: white;background-color: black;")
        # self.skinTitle.setFixedSize(300,30)
        
        self.startFrameLabel1 = QtWidgets.QLabel("Start frame")
        self.endFrameLabel2 = QtWidgets.QLabel("End frame");
        # self.displayInfLabel3 = QtWidgets.QLabel("Inf3");
        # self.displayInfLabel4 = QtWidgets.QLabel("Inf4");
        
        self.infLabelLayout = QtWidgets.QHBoxLayout()
        self.infLabelLayout.addWidget(self.startFrameLabel1)
        self.infLabelLayout.addWidget(self.endFrameLabel2)
        # self.infLabelLayout.addWidget(self.displayInfLabel3)
        # self.infLabelLayout.addWidget(self.displayInfLabel4)
        
        self.startFrame1 = QtWidgets.QLineEdit(str(int(pm.SCENE.defaultRenderGlobals.startFrame.get())))
        self.endFrame2 = QtWidgets.QLineEdit(str(int(pm.SCENE.defaultRenderGlobals.endFrame.get())))
        # self.displayWeight3 = QtWidgets.QLineEdit("0");
        # self.displayWeight4 = QtWidgets.QLineEdit("0");
        
        self.weightLayout = QtWidgets.QHBoxLayout()
        self.weightLayout.addWidget(self.startFrame1)
        self.weightLayout.addWidget(self.endFrame2)
        # self.weightLayout.addWidget(self.displayWeight3)
        # self.weightLayout.addWidget(self.displayWeight4)

        self.skinWeightGrid = QtWidgets.QGridLayout()
        self.skinWeightGrid.addLayout(self.infLabelLayout, 0, 0)
        self.skinWeightGrid.addLayout(self.weightLayout, 1, 0)

        self.groupLabel1 = QtWidgets.QLabel("Host Group:");
        self.group_list = QtWidgets.QComboBox()
        self.group_list.insertItems(0, ('allhosts', 'grafika', 'render', 'old_intel', 'new_intel') )

        self.queueLabel1 = QtWidgets.QLabel("Queue:");
        self.queue_list = QtWidgets.QComboBox()
        self.queue_list.insertItems(0, ('cuda',) )

        self.hold = QtWidgets.QCheckBox("Job on hold")

        self.priorityLabel = QtWidgets.QLabel("Priority")
        self.priority = QtWidgets.QLineEdit( str(const.hafarm_defaults['priority']) )
        self.priority.setFixedWidth(60)

        self.priorityLayout = QtWidgets.QHBoxLayout()
        self.priorityLayout.addWidget(self.priorityLabel)
        self.priorityLayout.addWidget(self.priority)

        self.priorityGrid = QtWidgets.QGridLayout()
        self.priorityGrid.addLayout(self.priorityLayout, 0, 0, QtCore.Qt.AlignLeft)

        self.excludeLabel1 = QtWidgets.QLabel("Exclude hosts list:");
        self.excludeHosts = QtWidgets.QLineEdit("")
        
        self.submitButton = QtWidgets.QPushButton("Submit")
        self.submitButton.clicked.connect(self.submit_render)
        self.submitButton.setFixedSize(50,20)
        
        self.mainLayout.addWidget(self.jointTitle)
        self.mainLayout.addWidget(self.cameraLabel1)
        self.mainLayout.addWidget(self.renderable_cameras)
        # self.mainLayout.addWidget(self.jointList)
        # self.mainLayout.addWidget(self.skinTitle)
        self.mainLayout.addLayout(self.skinWeightGrid)
        self.mainLayout.addWidget(self.groupLabel1)
        self.mainLayout.addWidget(self.group_list)
        self.mainLayout.addWidget(self.queueLabel1)
        self.mainLayout.addWidget(self.queue_list)
        self.mainLayout.addWidget(self.hold)
        self.mainLayout.addLayout(self.priorityGrid)
        # self.mainLayout.addWidget(self.color)
        # self.mainLayout.addWidget(self.position)
        self.mainLayout.addWidget(self.excludeLabel1)
        self.mainLayout.addWidget(self.excludeHosts)
        self.mainLayout.addWidget(self.submitButton)


    def submit_render(self):
        global_params = dict(
             queue = str(self.queue_list.currentText())
            ,image_render=True
            ,group = str(self.group_list.currentText())
            ,start_frame = int(self.startFrame1.text())
            ,end_frame = int(self.endFrame2.text())
            # ,frame_range = int(self.every_of_Knob.getValue())
            ,job_on_hold = bool(self.hold.isChecked())
            ,priority = int(self.priority.text())
            ,target_list = [ "\"%s\""%str(self.renderable_cameras.currentText()) ]
            ,exclude_list = [] if str(self.excludeHosts.text()) == '' else [x.strip() for x in str(self.excludeHosts.text()).split(',') ]
        )

        with HaContextMaya() as mactx:
            node = HaRedshiftRenderWrapper(**global_params)
            mactx.add_node(node)

        self.close()


