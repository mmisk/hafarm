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


from HaGraph import HaGraph
from HaGraph import HaGraphItem
from hafarm import SlurmRender

import const
import copy
from SimpleXMLRPCServer import *
import thread
import socket


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


# startSharedServer(["python", "/opt/package/rez_packages/hafarm/v0.3.3/py/hafarm/scripts/maya/shared_server.py",IP,PORT])


def save_to_scratch():
    scene_name = pm.sceneName()
    basename = os.path.basename(scene_name)
    scratch_name = tempfile.mkdtemp(suffix=basename, dir="/SCRATCH/temp")
    pm.renameFile(scratch_name)
    pm.saveFile()
    pm.renameFile(scene_name)
    return scratch_name


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


thread.start_new_thread(dispatchForever, (None,))



class USDstitchFarm(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/usd/stitch', '')
        dependencies = kwargs.get('deps',[])
        super(USDstitchFarm, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)
        
        script_name = kwargs.get('scenefilepath', pm.sceneName())
        path, name = os.path.split(script_name)
        basename, ext = os.path.splitext(name)

        out_filepath = kwargs['output_filepath']

        self.tags = '/maya/export_geo'
        self.parms['exe'] = 'rez env usd -- python'
        self.parms['req_resources'] = ''
        self.parms['slots'] = 1
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/usd_stitch.py', str(IP), str(PORT), kwargs['jbname'] + ext, out_filepath]
        self.parms['command'] << '{exe} {command_arg}' 
        self.parms['job_name'] << { "job_basename": kwargs['jobname']
                                    , "jobname_hash": kwargs['jobname_hash']
                                    , "render_driver_name": "abc"}
    
    def copy_scene_file(self, **kwargs):
        pass



class MayaFarm(HaGraphItem):
    def __init__(self, *args, **kwargs):
        index, name = str(uuid4()), 'maya'
        tags, path = ('/maya/farm', '')
        dependencies = []
        super(MayaFarm, self).__init__(index, dependencies, name, path, tags, *args, **kwargs)

    
    def makeAbc(self, scene_path, output_path, abc_args):
        """ Export alembic file.
        """
        file_name = os.path.basename(scene_path)
        self.parms['job_name'] = self.generate_unique_job_name(file_name)
        self.parms['scene_file'] = scene_path
        self.parms['output_picture'] = output_path
        self.parms['command_arg'] = abc_args
        self.parms['command'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 48
        self.parms['step_frame'] = 48


    def render(self, **kwargs):
        if kwargs.get('image_render') != None:
            self._redshiftRender(**kwargs)
        elif kwargs.get('geometry_render') != None:
            self._exportAbcAnimationByUSDframes(**kwargs)


    def _exportAbcAnimationByUSDframes(self, **kwargs):
        self.tags = '/maya/export_geo'
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/render_usd_frames.py']
        self.parms['command'] << ( '{exe} {command_arg} {scene_file} "{target_list}" %s %s' % (IP, PORT) )
        self.parms['queue'] = kwargs['queue']
        self.parms['group'] = kwargs['group']
        self.parms['start_frame'] = kwargs['start_frame']
        self.parms['end_frame'] = kwargs['end_frame']
        self.parms['step_frame'] = 20
        self.parms['frame_range_arg'] = [' %s %s','start_frame','end_frame']


        script_name = kwargs.get('scenefilepath', pm.sceneName())
        path, name = os.path.split(script_name)
        basename, ext = os.path.splitext(name)


        self.parms['job_name'] << { "job_basename": kwargs['jobname']
                                    , "jobname_hash": self.get_jobname_hash()
                                    , "render_driver_name": "usd"}
        self.parms['scene_file'] << { "scene_file_path": path
                                        ,"scene_file_basename": basename
                                        ,"scene_file_ext": ext }
        self.parms['target_list'] = kwargs['target_list']


    def _redshiftRender(self, **kwargs):
        self.parms['exe'] = '$MAYA_LOCATION/bin/mayapy'
        self.parms['command_arg'] = ['$HAFARM_HOME/scripts/maya/mayars.py']
        self.parms['command'] << '{exe} {command_arg} {scene_file} {target_list}'
        script_name = pm.sceneName()
        path, name = os.path.split(script_name)
        self.parms['scene_file'] << { 'scene_fullpath': script_name }
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



class HaContextMaya(object):
    def _get_graph(self, **kwargs):
        job = os.getenv('JOB_CURRENT', 'none')
        graph = HaGraph(graph_items_args=[])

        mayanode = MayaFarm(**kwargs)
        mayanode.render(**kwargs)
        graph.add_node(mayanode)

        jobname_hash = mayanode.parms['job_name'].data()['jobname_hash']

        if kwargs.get('geometry_render') != None:
            _kwargs = copy.copy(kwargs)

            _kwargs.update( {'deps':[mayanode.index]
                                    , 'jobname_hash': jobname_hash
                                    , 'jbname': str(mayanode.parms['job_name'])
                                    , 'output_filepath': _kwargs['usd_stitch_outfile'] } ) 
            usdfarm = USDstitchFarm(**_kwargs)
            graph.add_node(usdfarm)

        return graph



def maya_main_window():
    main_window_ptr = omui.MQtUtil.mainWindow()
    return wrapInstance(long(main_window_ptr), QtWidgets.QWidget)



class MayaFarmGUI(QtWidgets.QDialog):
    _ctx = HaContextMaya()

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


    def create_geometry_panel(self):
        self.setWindowTitle("Hafarm")
        self.setWindowFlags(QtCore.Qt.Tool)
        self.resize(300, 90) # re-size the window
        self.mainLayout = QtWidgets.QVBoxLayout(self)
        
        self.jointTitle = QtWidgets.QLabel(" Geometry render ")         
        self.jointTitle.setStyleSheet("color: white;background-color: black;")
        self.jointTitle.setFixedSize(300,30)

        self.geoLabel1 = QtWidgets.QLabel("Selected object:");
        self.geometry_path = QtWidgets.QTextEdit()
        self.geometry_path.setFixedSize(300,28)
        selection = pm.ls(selection=True)
        if not selection:
            selected_object = '<html><head/><body><p align="center" style="color:yellow;"><span style=" font-size:8.25pt;">Please, select object and reopen dialog....</span></p></body></html>'
        else:
            selected_object = str(selection[0])

        self.geometry_path.setHtml(selected_object)
       
        self.startFrameLabel1 = QtWidgets.QLabel("Start frame")
        self.endFrameLabel2 = QtWidgets.QLabel("End frame");
        # self.displayInfLabel3 = QtWidgets.QLabel("Inf3");
        # self.displayInfLabel4 = QtWidgets.QLabel("Inf4");
        
        self.infLabelLayout = QtWidgets.QHBoxLayout()
        self.infLabelLayout.addWidget(self.startFrameLabel1)
        self.infLabelLayout.addWidget(self.endFrameLabel2)
        # self.infLabelLayout.addWidget(self.displayInfLabel3)
        # self.infLabelLayout.addWidget(self.displayInfLabel4)
        
        self.startFrame1 = QtWidgets.QLineEdit(str(int(pm.playbackOptions(q=True,min=True))))
        self.endFrame2 = QtWidgets.QLineEdit(str(int(pm.playbackOptions(q=True,max=True))))
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
        self.queue_list.insertItems(0, ('3d',) )

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
        self.submitButton.clicked.connect(self.submit_geometry)
        self.submitButton.setFixedSize(50,20)
        
        self.mainLayout.addWidget(self.jointTitle)
        self.mainLayout.addWidget(self.geoLabel1)
        self.mainLayout.addWidget(self.geometry_path)
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


    def submit_geometry(self):
        global_params = dict(
             queue = str(self.queue_list.currentText())
            ,geometry_render=True
            ,group = str(self.group_list.currentText())
            ,start_frame = int(self.startFrame1.text())
            ,end_frame = int(self.endFrame2.text())
            # ,frame_range = int(self.every_of_Knob.getValue())
            ,job_on_hold = bool(self.hold.isChecked())
            ,priority = int(self.priority.text())
            ,target_list = [ str(self.geometry_path.toPlainText()) ]
            ,exclude_list = [] if str(self.excludeHosts.text()) == '' else [x.strip() for x in str(self.excludeHosts.text()).split(',') ]
        )

        graph = self._ctx._get_graph(**global_params)
        graph.set_render(SlurmRender.SlurmRender)
        graph.render()

        print global_params

        self.close()


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
            ,target_list = [ str(self.renderable_cameras.currentText()) ]
            ,exclude_list = [] if str(self.excludeHosts.text()) == '' else [x.strip() for x in str(self.excludeHosts.text()).split(',') ]
        )

        graph = self._ctx._get_graph(**global_params)
        graph.set_render(SlurmRender.SlurmRender)
        graph.render()

        print global_params

        self.close()


