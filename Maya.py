import os
from uuid import uuid4

from PySide2 import QtCore
from PySide2 import QtGui
from PySide2 import QtWidgets
from PySide2 import __version__
from shiboken2 import wrapInstance 
from maya import OpenMayaUI as omui 
import pymel.core as pm


from HaGraph import HaGraph
from HaGraph import HaGraphItem
from hafarm import SlurmRender

import const


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
        self._redshiftRender(**kwargs)


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
        
        # self.parms['output_picture'] = kwargs['output_picture']
        self.parms['job_on_hold'] = kwargs['job_on_hold']
        self.parms['priority'] = kwargs['priority']
        self.parms['target_list'] = kwargs['target_list']



class HaContextMaya(object):
    def _get_graph(self, **kwargs):
        job = os.getenv('JOB_CURRENT', 'none')
        graph = HaGraph(graph_items_args=[])
        
        # if not 'target_list' in kwargs:
        #     kwargs['target_list'] = [x.name() for x in write_node_list ]

        # if not 'output_picture' in kwargs:
        #     kwargs['output_picture'] = str(nuke.root().node(kwargs['target_list'][0]).knob('file').getEvaluatedValue())
        mayanode = MayaFarm(**kwargs)
        mayanode.render(**kwargs)
        graph.add_node(mayanode)
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

    def create(self):
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
        
        self.submitButton = QtWidgets.QPushButton("Submit")
        self.submitButton.clicked.connect(self.submit)
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
        self.mainLayout.addWidget(self.submitButton)


    def submit(self):
        global_params = dict(
             queue = str(self.queue_list.currentText())
            ,group = str(self.group_list.currentText())
            ,start_frame = int(self.startFrame1.text())
            ,end_frame = int(self.endFrame2.text())
            # ,frame_range = int(self.every_of_Knob.getValue())
            ,job_on_hold = bool(self.hold.isChecked())
            ,priority = int(self.priority.text())
            ,target_list = [ str(self.renderable_cameras.currentText()) ]
        )

        # if self.requestSlots_Knob.value():
        #     global_params.update( {'req_resources': 'procslots=%s' % int(self.slotsKnob.value()) } )
        
        # if self.email_list.value():
        #     global_params.update( {'email_list': [utils.get_email_address()]} )

        graph = self._ctx._get_graph(**global_params)
        graph.set_render(SlurmRender.SlurmRender)
        graph.render()

        print global_params

        self.close()


