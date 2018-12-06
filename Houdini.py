# Standard:
import re
import os
import itertools
import time
import glob
# Host specific:
import hou

# Custom: 
import utils
import const
import Batch
from Batch import BatchMp4, BatchDebug, BatchReportsMerger, BatchJoinTiles

from uuid import uuid4

import HaGraph
from HaGraph import HaGraph
from HaGraph import HaGraphItem

import parms
from parms import HaFarmParms

houdini_dependencies = {}


def get_houdini_render_nodes(hafarm_node_path):
    hscript_out = hou.hscript('render -pF %s' % hafarm_node_path )
    ret = []
    for item in hscript_out[0].strip('\n').split('\n'):
        index, deps, path, frames  = re.match('(\\d+) \\[ ([0-9\\s]+)?\\] ([a-z/0-9A-Z_]+)\\s??([0-9\\s]+)', item).groups()
        deps = [] if deps == None else deps.split(' ')
        deps = [str(x) for x in deps if x != '']
        houdini_dependencies[index] = deps
        hou_node_type = hou.node(path).type().name()
        ret += [ (hou_node_type, index, deps, path) ]
    return ret



class HoudiniNodeWrapper(HaGraphItem):
    def __init__(self, index, path, depends, **kwargs):
        self._kwargs = kwargs
        self._slice_idx = kwargs.get('_slice_idx', 0)
        self.name = path.rsplit('/', 1)[1]
        super(HoudiniNodeWrapper, self).__init__(index, depends, self.name, path, '')
        self._make_proxy = kwargs.get('make_proxy', False)
        self._make_movie = kwargs.get('make_movie', False)
        self._debug_images = kwargs.get('debug_images', False)
        self._resend_frames = kwargs.get('resend_frames', False)
        self.path = path
        self.hou_node = hou.node(path)
        self.hou_node_type = self.hou_node.type().name()
        self.tags = '/houdini/%s' % self.hou_node_type
        self._slices = [1]
        self._indices = map(lambda x: str(uuid4()), self._slices)
        self._indices[0] = index
        self._instances = kwargs.get('instances', [])
        self.parms['output_picture'] = self.get_output_picture()
        self.parms['email_list']  = [utils.get_email_address()]
        self.parms['ignore_check'] = kwargs.get('ignore_check', True)
        self._scene_file = str(hou.hipFile.name())
        path, name = os.path.split(self._scene_file)
        basename, ext = os.path.splitext(name)
        self.parms['scene_file'] << { "scene_file_path": path
                                        ,"scene_file_basename": name
                                        ,"scene_file_ext": ext }
        self.parms['job_name'] << { "job_basename": name
                                    , "jobname_hash": self.get_jobname_hash()
                                    , "render_driver_type": self.hou_node_type
                                    , "render_driver_name": self.hou_node.name() }


    def __iter__(self):
        for slice_idx, index in enumerate(self._indices):
            self._kwargs['_slice_idx'] = slice_idx
            self._kwargs['instances'] = self._instances
            x = type(self)(index, self.path, self.get_dependencies(), **self._kwargs)
            self._instances += [ x ]
            yield x


    def post_render_actions(self):
        return []


    def get_output_picture(self):
        return ''


    def get_step_frame(self):
        return  int(self.rop.parm('f2').eval()) if self._kwargs.get('use_one_slot') else self._kwargs.get('step_frame')


    def _proxy_post_render(self):
        post_renders = []
        self.parms['command'] << {'proxy': ' --proxy '}

        if self._make_movie == True:
            make_movie_action = BatchMp4( self.parms['output_picture']
                                          , job_data = self.parms['job_name'].data())
            make_movie_action.add(self)
            post_renders += [ make_movie_action ]

        return post_renders


    def _debug_post_render(self):
        post_renders = []

        debug_render = BatchDebug( self.parms['output_picture']
                                    , job_data = self.parms['job_name'].data()
                                    , start = self.parms['start_frame']
                                    , end = self.parms['end_frame'] )
        debug_render.add(self)
        post_renders += [ debug_render ]

        ifd_path = os.path.join(os.getenv("JOB"), 'render/sungrid/ifd')
        
        merger = BatchReportsMerger( self.parms['output_picture']
                                        , job_data = self.parms['job_name'].data()
                                        , ifd_path = ifd_path
                                        , resend_frames = self._resend_frames )
        merger.add(debug_render)
        post_renders += [ merger ]
        return post_renders



class HbatchWrapper(HoudiniNodeWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HbatchWrapper, self).__init__(index, path, depends, **kwargs)
        use_frame_list = kwargs.get('use_frame_list')
        self.hbatch_slots = kwargs.get('hbatch_slots')

        self.parms['command'] << { 'command': '$HFS/bin/hython' }
        self.parms['command_arg'] = [kwargs.get('command_arg')]
        self.parms['req_license'] = 'hbatch_lic=1' 
        self.parms['req_resources'] = 'procslots=%s' % self.hbatch_slots
        self.parms['target_list'] = [str(self.hou_node.path()),]
        self.parms['step_frame'] = self.get_step_frame()
        self.parms['start_frame'] = int(self.hou_node.parm('f1').eval())
        self.parms['end_frame']  = int(self.hou_node.parm('f2').eval())
        self.parms['frame_range_arg'] = ["-f %s %s -i %s", 'start_frame', 'end_frame', int(self.hou_node.parm('f3').eval())]
        self.parms['req_memory'] = kwargs.get('hbatch_ram')
        if use_frame_list:
            self.parms['frame_list'] = kwargs.get('frame_list')
            self.parms['step_frame'] = int(self.hou_node.parm('f2').eval())
            self.parms['command_arg'] += ['-l %s' %  self.parms['frame_list']]
        self.parms['command_arg'] += ['-d %s' % " ".join(self.parms['target_list'])]
        command_arg = [ "--ignore_tiles", "--generate_ifds" ]
        if kwargs.get('ifd_path_is_default') == None:
            command_arg += ["--ifd_path %s" % kwargs.get('ifd_path')]

        for x in command_arg[::-1]:
            self.parms['command_arg'].insert(1, x)


    def pre_schedule(self):
        if self.hbatch_slots:
            self.parms['command_arg'] += ['-j %s' % self.parms['slots']]



class HoudiniRSWrapper(HbatchWrapper):
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniRSWrapper, self).__init__(index, path, depends, **kwargs)
        self.name += '_rs'
        self.parms['req_license'] = 'hbatch_lic=1,redshift_lic=1'
        self.parms['queue'] = 'cuda'
        self.parms['job_name'] << { 'jobname_hash': kwargs.get('ifd_hash'), 'render_driver_type': 'rs' }
        ifd_name = self.parms['job_name'].clone()
        ifd_name << { 'render_driver_type': '' }
        self.parms['command_arg'] += ["--ifd_name %s" %  ifd_name ]

    def get_output_picture(self):
        return self.hou_node.parm('RS_outputFileNamePrefix').eval()



class HoudiniRedshiftROPWrapper(HoudiniNodeWrapper):
    """docstring for HoudiniRedshiftROPWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniRedshiftROPWrapper, self).__init__(index, path, depends, **kwargs)
        self.name += '_redshift'
        self.parms['queue'] = 'cuda' 
        self.parms['command'] << { 'command': '$REDSHIFT_COREDATAPATH/bin/redshiftCmdLine' }
        self.parms['req_license'] = 'redshift_lic=1'
        self.parms['req_memory'] = kwargs.get('mantra_ram')
        self.parms['pre_render_script'] = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HFS/dsolib"
        self.parms['scene_file'] << { 'scene_file_path': kwargs['ifd_path'],  'scene_file_ext': '.rs' }
        self.parms['job_name'] << { 'render_driver_type': 'redshift' }

        if 'ifd_hash' in kwargs:
            self.parms['job_name'] << { 'jobname_hash': kwargs['ifd_hash'] }
            self.parms['scene_file'] << { 'scene_file_hash': kwargs['ifd_hash'] + '_' + self.parms['job_name'].data()['render_driver_name'] }


    def __iter__(self):
        self._kwargs['ifd_hash'] = self.get_jobname_hash()
        rs = HoudiniRSWrapper(str(uuid4()), self.path, [x for x in self.dependencies], **self._kwargs)
        self._instances += [rs]
        yield rs

        pieces = [self.index] + map(lambda _: str(uuid4()), self._slices)[1:]
        for n in pieces:
            rsrop = HoudiniRedshiftROPWrapper(n, self.path, [rs.index], **self._kwargs)
            self._instances += [rsrop]
            rsrop._instances = self._instances
            yield rsrop


    def get_output_picture(self):
        return self.hou_node.parm('RS_outputFileNamePrefix').eval()


    def copy_scene_file(self, **kwargs):
        ''' It is not clear enough in this place :(
            but mantra should skip copy scene file 
            because of input file is *.@TASK_ID/>.ifd
            and copyfile function mess up it
        '''
        pass



class HoudiniBaketexture(HbatchWrapper):
    def get_output_picture(self):
        return self.hou_node.parm('vm_uvoutputpicture1').eval()



class HoudiniBaketexture30(HbatchWrapper):
    def get_output_picture(self):
        return self.hou_node.parm('vm_uvoutputpicture1').eval()



class HoudiniIFDWrapper(HbatchWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniIFDWrapper, self).__init__(index, path, depends, **kwargs)
        self.name += '_ifd'
        self.parms['job_name'] << { 'jobname_hash': kwargs['ifd_hash'], 'render_driver_type': 'ifd' }
        ifd_name = self.parms['job_name'].clone()
        ifd_name << { 'render_driver_type': '' }
        self.parms['command_arg'] += ["--ifd_name %s" %  ifd_name ]

    def get_output_picture(self):
        return self.hou_node.parm('vm_picture').eval()



class HoudiniMantraExistingIfdWrapper(HoudiniNodeWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        self._output_picture = kwargs.get('output_picture','')
        super(HoudiniMantraExistingIfdWrapper, self).__init__(index, path, depends, **kwargs)
        self.name += '_render'
        threads = kwargs.get('mantra_slots')
        if self.parms['cpu_share'] != 1.0:
            self.parms['command_arg'] = ['-j', const.MAX_CORES]
        else:
            self.parms['command_arg'] = ['-j', str(threads)]

        self.parms['job_name'] << { "jobname_hash": self.get_jobname_hash() }
        self.parms['command'] << { 'command': '$HFS/bin/mantra' }
        self.parms['command_arg'] += ["-V1", "-f", "@SCENE_FILE/>"]
        self.parms['slots'] = threads
        self.parms['req_license'] = 'mantra_lic=1'
        self.parms['req_memory'] = kwargs.get('mantra_ram')
        self.parms['start_frame'] = kwargs.get('start_frame', 0)
        self.parms['end_frame'] = kwargs.get('end_frame', 0)

        if kwargs.get('render_exists_ifd'):
            self.parms['output_picture'] = kwargs.get('output_picture')


    def get_output_picture(self):
        return self._output_picture


    def copy_scene_file(self, **kwargs):
        ''' It is not clear enough in this place :(
            but mantra should skip copy scene file 
            because of input file is *.@TASK_ID/>.ifd
            and copyfile function mess up it
        '''
        pass



class HoudiniMantraWrapper(HoudiniMantraExistingIfdWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniMantraWrapper, self).__init__(index, path, depends, **kwargs)
        mantra_filter = kwargs.get('mantra_filter')
        frame = None
        self._slices = kwargs.get('frames')
        self._tiles_x, self._tiles_y = kwargs.get('tile_x'), kwargs.get('tile_y')
        self._vm_tile_render = self.hou_node.parm('vm_tile_render').eval()
        if self._tiles_x * self._tiles_y > 1:
            self.name += '_tiles'
            self._vm_tile_render = True
        elif self._vm_tile_render:
            self.name += '_tiles'
            self._tiles_x = self.hou_node.parm('vm_tile_count_x').eval()
            self._tiles_y = self.hou_node.parm('vm_tile_count_y').eval()
        else:
            self.parms['command'] << { 'mantra_filter': mantra_filter }
        self.parms['tile_x'] = self._tiles_x
        self.parms['tile_y'] = self._tiles_y
        self.parms['command'] << { 'command' : '$HFS/bin/' +  str(self.hou_node.parm('soho_pipecmd').eval()) }
        self.parms['start_frame'] = frame if frame else int(self.hou_node.parm('f1').eval())
        self.parms['end_frame'] = frame if frame else int(self.hou_node.parm('f2').eval())

        if kwargs.get('render_exists_ifd'):
            self.parms['scene_file'] << { 'scene_fullpath': kwargs.get('scene_file') }
            self.parms['output_picture'] = kwargs.get('output_picture')

        self.parms['scene_file'] << { 'scene_file_path': kwargs['ifd_path'], 'scene_file_ext': '.ifd' }
        self.parms['job_name'] << { 'render_driver_type': 'mantra' }

        if 'ifd_hash' in kwargs:
            self.parms['job_name'] << { 'jobname_hash': kwargs['ifd_hash'] }
            self.parms['scene_file'] << { 'scene_file_hash': kwargs['ifd_hash'] + '_' + self.parms['job_name']._data['render_driver_name'] }


    def get_step_frame(self):
        return self.hou_node.parm("ifd_range3").eval()


    def __iter__(self):
        self._kwargs['ifd_hash'] = self.get_jobname_hash()
        ifd = HoudiniIFDWrapper(str(uuid4()), self.path, [x for x in self.dependencies], **self._kwargs)
        self._instances += [ifd]
        yield ifd

        pieces = [self.index] + map(lambda _: str(uuid4()), self._slices)[1:]
        for n in pieces:
            mtr = HoudiniMantraWrapper(n, self.path, [ifd.index], **self._kwargs)
            self._instances += [mtr]
            mtr._instances = self._instances
            yield mtr


    def get_output_picture(self):
        return self.hou_node.parm('vm_picture').eval()


    def post_render_actions(self):
        post_renders = []

        if self._slices != [1]:
            self._frames_render()

        if self._vm_tile_render == True:
           post_renders += self._tile_post_render()

        if self._make_proxy == True:
            post_renders += self._proxy_post_render()

        if self._debug_images == True:
            post_renders += self._debug_post_render()
            
        return post_renders


    def _frames_render(self):
        self.parms['job_name'] << { "frame": self._slices[self._slice_idx] }
        mantra_instances = filter(lambda x: isinstance(x, HoudiniMantraWrapper), self._instances)

        for k, m in houdini_dependencies.iteritems():
            if self._instances[1].index in m: # It is not clear that in __iter__() function instances look like that [ifd.index, root.index, rest.index, ...] 
                houdini_dependencies[k] += [ x.index for x in mantra_instances if not x.index in m ]


    def _tile_post_render(self):
        '''Generates merge job with general BatchFarm class for joining tiles.'''
        post_renders = []
        TILES_SUFFIX = "_tile%02d_"
        
        filepath, padding, ext = self.parms['output_picture'].rsplit('.',2)
        path, basename = os.path.split(filepath)

        mask_filename = { 'scene_file_ext': '.' + ext, 'scene_file_path': path, 'scene_file_basename': basename + '.%s' }
        output_picture = '.'.join([filepath + TILES_SUFFIX, const.TASK_ID, ext])
        self.parms['output_picture'] = output_picture
        self.parms['job_name'] << { 'tiles' : True }

        join_tiles_action = BatchJoinTiles( '.'.join([filepath, const.TASK_ID, ext])
                                            , self._tiles_x, self._tiles_y
                                            , mask_filename
                                            , self.parms['priority'] + 1
                                            , make_proxy = self._make_proxy 
                                            , start = self.parms['start_frame']
                                            , end = self.parms['end_frame']
                                            , job_data = self.parms['job_name'].data()
                                        )

        mantra_instances = filter(lambda x: isinstance(x, HoudiniMantraWrapper), self._instances)
        self.index, join_tiles_action.index = join_tiles_action.index, self.index
        join_tiles_action.add( *mantra_instances )
        post_renders += [ join_tiles_action ]

        return post_renders


    
class HoudiniAlembicWrapper(HbatchWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniAlembicWrapper, self).__init__(index, path, depends, **kwargs)

    def get_step_frame(self):
        return int(self.hou_node.parm('f2').eval())

    def get_output_picture(self):
        return self.hou_node.parm('filename').eval()



class HoudiniGeometryWrapper(HbatchWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniGeometryWrapper, self).__init__(index, path, depends, **kwargs)

    def get_output_picture(self):
        return self.hou_node.parm('sopoutput').eval()



class HoudiniCompositeWrapper(HbatchWrapper):
    """docstring for HaMantraWrapper"""
    def __init__(self, index, path, depends, **kwargs):
        super(HoudiniCompositeWrapper, self).__init__(index, path, depends, **kwargs)


    def get_output_picture(self):
        return self.hou_node.parm('copoutput').eval()


    def post_render_actions(self):
        post_renders = []

        if self._make_proxy == True:
            post_renders += self._proxy_post_render()

        if self._debug_images == True:
            post_renders += self._debug_post_render()
            
        return post_renders



class HoudiniWrapper(type):
    """docstring for HaHoudiniWrapper"""
    def __new__(cls, name, *args, **kwargs):
        hou_drivers = {   'ifd': HoudiniMantraWrapper
                        , 'baketexture':  HoudiniBaketexture
                        , 'baketexture::3.0':  HoudiniBaketexture30                        
                        , 'alembic': HoudiniAlembicWrapper
                        , 'geometry': HoudiniGeometryWrapper
                        , 'comp': HoudiniCompositeWrapper
                        , 'Redshift_ROP': HoudiniRedshiftROPWrapper
                    }
        return hou_drivers[name](*args, **kwargs)



class HaContextHoudini(object):
    def _get_graph(self, **kwargs):
        hafarm_node = hou.pwd()
        if hafarm_node.type().name() != 'HaFarm':
            raise Exception('Please, select the HaFarm node.')

        use_frame_list = hafarm_node.parm("use_frame_list").eval()
        frames = [1]
        if use_frame_list == True:
            frames = hafarm_node.parm("frame_list").eval()
            frames = utils.parse_frame_list(frames)

        tile_x, tile_y = 1, 1
        if hafarm_node.parm('tiles').eval() == True:
            tile_x, tile_y = hafarm_node.parm('tile_x').eval(), hafarm_node.parm('tile_y').eval()
        
        global_parms = dict(
                  queue = str(hafarm_node.parm('queue').eval())
                , group = str(hafarm_node.parm('group').eval())
                , job_on_hold = bool(hafarm_node.parm('job_on_hold').eval())
                , priority = int(hafarm_node.parm('priority').eval())
                , ignore_check = True if hafarm_node.parm("ignore_check").eval() else False
                , email_list  = [utils.get_email_address()] #+ list(hafarm_node.parm('additional_emails').eval().split()) if hafarm_node.parm("add_address").eval() else []
                , email_opt  = str(hafarm_node.parm('email_opt').eval())
                , req_start_time = hafarm_node.parm('delay').eval()*3600
                , frame_range_arg = ["%s%s%s", '', '', '']
                , resend_frames = hafarm_node.parm('rerun_bad_frames').eval()
                , step_frame = hafarm_node.parm('step_frame').eval()
                , ifd_path = hafarm_node.parm("ifd_path").eval()
                , frames = frames
                , use_frame_list = use_frame_list
                , make_proxy = bool(hafarm_node.parm("make_proxy").eval())
                , make_movie = hafarm_node.parm("make_movie").eval()
                , debug_images = hafarm_node.parm("debug_images").eval()
                , mantra_filter = hafarm_node.parm("ifd_filter").eval()
                , tile_x = tile_x
                , tile_y = tile_y
                , cpu_share = hafarm_node.parm("cpu_share").eval()
                , max_running_tasks = const.hafarm_defaults['max_running_tasks']
                , mantra_slots = const.hafarm_defaults['slots']
                , mantra_ram = const.hafarm_defaults['req_memory']
                , hbatch_slots = const.hafarm_defaults['slots']
                , hbatch_ram = const.hafarm_defaults['req_memory']
            )

        task_control = {}

        if hafarm_node.parm('more').eval() == True:
            task_control = dict(
                      max_running_tasks = hafarm_node.parm('max_running_tasks').eval()
                    , mantra_slots = int(hafarm_node.parm('mantra_slots').eval())
                    , mantra_ram = hafarm_node.parm("mantra_ram").eval()
                    , hbatch_slots = hafarm_node.parm('hbatch_slots').eval()
                    , hbatch_ram = hafarm_node.parm('hbatch_ram').eval()
                )

        global_parms.update(task_control)

        hou.hipFile.save()
        
        clsctx = None
        render_from_ifd = hafarm_node.parm("render_from_ifd").eval()
        if render_from_ifd == True:
            clsctx = HaContextHoudiniExistingIfd(hafarm_node, global_parms)
        else:
            clsctx = HaContextHoudiniMantra(hafarm_node, global_parms)
        return clsctx._get_graph(**kwargs)


    def pre_render(self):
        hou.allowEnvironmentToOverwriteVariable('JOB', True)
        hou.hscript('set JOB=' + os.environ.get('JOB'))



class HaContextHoudiniExistingIfd(object):
    def __init__(self, hafarm_node, global_parms):
        self.hafarm_node = hafarm_node
        self.global_parms = global_parms


    def _get_ifd_files(self):
        ifds  = self.hafarm_node.parm("ifd_files").eval()
        ifds = ifds.strip()
        if not os.path.exists(ifds):
            raise Exception('Error! Ifd file not found: "%s"'%ifds)
        # Rediscover ifds:
        # FIXME: should be simple unexpandedString()
        seq_details = utils.padding(ifds)
        # Find real file sequence on disk. Param could have $F4...
        real_ifds = glob.glob(seq_details[0] + "*" + seq_details[-1])
        real_ifds.sort()
        if real_ifds == []:
            print "Can't find ifds files: %s" % ifds
        return real_ifds, os.path.split(seq_details[0])[1], seq_details[0] + const.TASK_ID + '.ifd'


    def _get_graph(self, **kwargs):
        graph = HaGraph(graph_items_args=[])

        real_ifds, name_prefix, scene_file = self._get_ifd_files()
        if real_ifds == []:
            return graph

        if self.global_parms['use_frame_list'] == False:
            frames = xrange(self.hafarm_node.parm("ifd_range1").eval(), self.hafarm_node.parm("ifd_range2").eval())
            self.global_parms['frames'] = frames

        params_for_node_wrappers = dict(  output_picture = utils.get_ray_image_from_ifd(real_ifds[0])
                                        , scene_file = scene_file
                                        , name_prefix = name_prefix
                                        , render_exists_ifd = True
                                        , start_frame = self.hafarm_node.parm("ifd_range1").eval()
                                        , end_frame = self.hafarm_node.parm("ifd_range2").eval()
                                    )
        self.global_parms.update(params_for_node_wrappers)
        item = HoudiniMantraExistingIfdWrapper( str(uuid4()), self.hafarm_node.path(), [], **self.global_parms )
        graph.add_node( item )
        return graph


class HaContextHoudiniMantra(object):
    def __init__(self, hafarm_node, global_parms):
        self.hafarm_node = hafarm_node
        self.global_parms = global_parms


    def _get_graph(self, **kwargs):
        params_for_node_wrappers = dict(
                  ifd_path_is_default = self.hafarm_node.parm("ifd_path").isAtDefault()
                , use_one_slot = self.hafarm_node.parm('use_one_slot').eval()
                , command_arg = self.hafarm_node.parm('command_arg').eval()
                , frame_list = str(self.hafarm_node.parm("frame_list").eval())
            )

        graph = HaGraph(graph_items_args=[])
        self.global_parms.update(params_for_node_wrappers)
        for x in get_houdini_render_nodes(self.hafarm_node.path()):
            hou_node_type, index, deps, path = x
            for item in HoudiniWrapper( hou_node_type, index, path, houdini_dependencies[index], **self.global_parms ):
                graph.add_node( item  )
                for post in item.post_render_actions():
                    graph.add_node( post )
        return graph
