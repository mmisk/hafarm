#!/usr/bin/env python
# coding:utf-8

import os
import json
import sys

# meshroom_submit --toNode Meshing --submitter Slurm /tmp/meshroom_rock.mg

sys.path.insert(0,os.path.expandvars('$REZ_HAFARM_ROOT/py'))

from uuid import uuid4
from hafarm.HaGraph import HaGraph
from hafarm.HaGraph import random_hash_string
from hafarm.HaGraph import HaGraphItem
from hafarm import SlurmRender
from hafarm import PrintRender
from hafarm import const

import meshroom
from meshroom.core.desc import Level
from meshroom.core.submitter import BaseSubmitter

SCRATCH = os.environ['MESHROOM_CACHE']

class MeshroomNodeWrapper(HaGraphItem):
    def __init__(self, index, depends, filepath, node, **kwargs):
        self._kwargs = kwargs
        self.name = node.name
        super(MeshroomNodeWrapper, self).__init__(index, depends, node.name, node.name, '')
        self.index = index
        self.meshroom_node = node
        self.tags = '/meshroom/%s' % self.meshroom_node.name
        self.parms['ignore_check'] = True
        # self.parms['job_on_hold'] = True
        self.parms['queue'] = 'cuda'
        self.parms['job_wait_dependency_entire'] = True
        path, name = os.path.split(filepath)
        basename, ext = os.path.splitext(name)
        self.parms['scene_file'] << { "scene_file_path": path
                                        ,"scene_file_basename": basename
                                        ,"scene_file_ext": ext }
        jobname_hash = kwargs.get('jobname_hash' , self.get_jobname_hash())
        self.parms['job_name'] << { "job_basename": basename
                                    , "jobname_hash": jobname_hash
                                    , "render_driver_type": 'mg'
                                    , "render_driver_name": self.meshroom_node.name }
        self.parms['exe'] = 'meshroom_compute'
        self.parms['target_list'] = [self.meshroom_node.name]
        self.parms['command'] << '{exe} --node {target_list} {scene_file} {command_arg} --extern'
        self.parms['command_arg'] = [ '--cache %s' % SCRATCH ]
        self.parms['start_frame'] = 1
        self.parms['end_frame'] = 1

        parallelArgs = []
        if self.meshroom_node.isParallelized:
            blockSize, fullSize, nbBlocks = self.meshroom_node.nodeDesc.parallelization.getSizes(self.meshroom_node)
            self.parms['step_frame'] = 1
            self.parms['start_frame'] = 0
            self.parms['end_frame'] = nbBlocks - 1
            if self.parms['end_frame'] > self.parms['step_frame']:
                self.parms['command_arg'].insert(0, '--iteration %s' % const.TASK_ID)



class SlurmFarmSubmitter(BaseSubmitter):
    def __init__(self, parent=None):
        super(SlurmFarmSubmitter, self).__init__(name='Slurm', parent=parent)


    def submit(self, nodes, edges, filepath):
        name = os.path.splitext(os.path.basename(filepath))[0] + ' [Meshroom]'
        graph = HaGraph(graph_items_args=[])

        indeces = {}
        for node in nodes:
            indeces[node.name] = str(uuid4())

        dependencies = {}
        for u, v in edges:
            dependencies[u.name] = [indeces[v.name]]

        jobname_hash = random_hash_string()

        for node in nodes:
            idx = indeces[node.name]
            deps = dependencies.get(node.name, [])
            item = MeshroomNodeWrapper( idx, deps, filepath, node, jobname_hash=jobname_hash )
            graph.add_node( item )

        # graph.set_render(PrintRender.JsonParmRender)
        graph.set_render(SlurmRender.SlurmRender)
        graph.render()

        return  True
