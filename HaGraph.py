import os
import shutil

import parms
from parms import HaFarmParms

from time import time
import random
import string
import json
from const import ConstantItemJSONEncoder

class HaGraphDependency(list):
    _all_dependencies = {}
    def __init__(self, key, data=[], parent=None):
        self._data = data
        self._parent = parent
        self._all_dependencies[key] = parent


    def __getitem__(self, index):
        return self._data[index]


    def __contains__(self, val):
        return val in self._data


    def __iadd__(self, val):
        self._data += val
        return self


    def remove(self, val):
        self._data.remove(val)


    def __iter__(self):
        for x in self._data:
            yield x


    def __len__(self):
        return len(self._data)


    def __str__(self):
        return "D(%s)" % self._data



class HaGraphItem(object):
    _slots = {}
    def __init__(self, index, dependencies, name, path, tags, **kwargs):
        self.index = index
        self.dependencies = HaGraphDependency(index, dependencies, self)
        self.name = name
        self.path = path
        self.tags = tags
        self.parms = HaFarmParms(initilize=True)


    def add(self, *graph_items, **kwargs):
        for n in graph_items:
            self.dependencies += [n.index]


    def get_dependencies(self):
        return self.dependencies


    def pre_schedule(self):
        """ This method is called automatically before job submission by HaFarm.
            Up to now:
            1) All information should be aquired from host application.
            2) They should be placed in HaFarmParms class (self.parms).
            3) Scene should be ready to be copied to handoff location.
            
            Main purpose is to prepare anything specific that HaFarm might not know about, 
            like renderer command and arguments used to render on farm's machines.
        """
        pass


    def copy_scene_file(self, **kwargs):
        """Makes a copy of a scene file.
        """
        scene_file = kwargs.get('scene_file', str(self.parms['scene_file']))
        # TODO: Currenty scene file is copied into job script directory
        # We might want to customize it, along with the whole idea of
        # coping scene. 
        filename, ext  = os.path.splitext(scene_file)
        path           = os.path.expandvars(self.parms['script_path'])
        self.parms['scene_file'] << { 'scene_file_path': path, 'scene_file_basename': str(self.parms['job_name']), 'scene_file_ext': ext }
        error = None
        new_scene_file = os.path.join(path, str(self.parms['job_name'])) + ext
       
        # We do either file copy or link copy. The latter one is less expensive
        # but less safe also, as we do use render cache as backup history from
        # time to time... :/
        try:
            if os.path.islink(scene_file):
                linkto = os.readlink(scene_file)
                os.symlink(linkto, new_scene_file)
            elif scene_file != new_scene_file:
                shutil.copy2(scene_file, new_scene_file)
            else:
                # self.logger.debug("Scene file already copied. %s " % new_scene_file)
                pass
        except (IOError, os.error), why:
            # self.logger.debug('%s: %s' % (new_scene_file, why))
            error = why
            new_scene_file = None

        return {'copy_scene_file': new_scene_file, 'error': error}


    def get_jobname_hash(self):
        return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(4))


    def generate_unique_job_name(self, name='no_name_job'):
        """Returns unique name for a job. 'Name' is usually a scene file. 
        """
        name = os.path.basename(name)
        return "_".join([os.path.split(name)[1], self.get_jobname_hash()])


    def __repr__(self):
        return 'HaGraphItem("%s",%s,"%s","%s","%s")' % \
            (self.index, self.dependencies, self.name, self.path, self.tags)


    def __str__(self):
        return '{"name":"%s", "index":%s, "tags":%s, path:"%s"}' % \
            (self.name, self.index, self.tags, self.path)



class HaGraph(object):
    def __init__(self, graph_items_args=[]):
        self.RenderCls = None
        self.graph_items = graph_items_args
        self.global_parms = HaFarmParms(initilize=True)


    # def tree(self):
    #     ret = {}

    #     return ret


    def set_render(self, render_cls):
        self.RenderCls = render_cls


    def add_node(self, *graph_items):
        for item in graph_items:
            for n, m in self.global_parms.iteritems():
                if not n in item.parms:
                    item.parms[n] = m

            self.graph_items += [ item ]

        
    def render(self, **kwargs):
        graph_items = {}
        for x in self.graph_items:
            x.copy_scene_file()
            x.pre_schedule()
            graph_items.update( {x.index: x} )

        for k, item in graph_items.iteritems():
            item.parms['submission_time'] = time()
            _db = {}
            _db['inputs'] = [ graph_items[x].parms['job_name'] for x in item.dependencies ]
            _db['class_name'] = item.__class__.__name__
            _db['backend_name'] = 'JsonParmRender'
            _db['parms'] = item.parms
            parms_file = os.path.expandvars(item.parms['script_path'])
            parms_file = os.path.join(parms_file, item.parms['job_name']) + '.json'
            with open(parms_file, 'w') as file:
                result = json.dump(_db, file, indent=2, cls=ConstantItemJSONEncoder)

        render = self.RenderCls(graph_items)
        render.render()


