# 2018.11.09 13:50:57 CET
#Embedded file name: hafarm/const.py
TASK_ID = '@TASK_ID/>'
RAND_LEN = 6
TILE_ID = '__TILE__'
OIIOTOOL = '/opt/packages/oiio-1.4.15/bin/oiiotool.sh'
MANTRA_FILTER = '/STUDIO/houdini/houdini13.0/scripts/python/HaFilterIFD_v01.py'
PROXY_POSTFIX = 'proxy'
TILES_POSTFIX = 'tiles'
DEBUG_POSTFIX = 'debug'
MAX_CORES = '@MAX_CORES/>'
IINFO = '$HFS/bin/iinfo'
DEBUG = 1
TASK_ID_PADDED = '@TASK_ID_PADDED/>'
SCENE_FILE = '@SCENE_FILE/>'
RENDER_WRANGERS = ['renderfarm@human-ark.com']
HAFARM_DEFAULT_BACKEND = 'Slurm'

import os
import json
from jinja2 import Environment, FileSystemLoader
import StringIO as io

TEMPLATE_ENVIRONMENT = Environment(autoescape=False, loader=FileSystemLoader(os.path.expandvars('$HAFARM_HOME/')), trim_blocks=False)
parms_jinja_template = TEMPLATE_ENVIRONMENT.get_template('parms.schema')

class ConstantItemJSONEncoder(json.JSONEncoder):

    def default(self, z):
        if isinstance(z, ConstantItem):
            return str(z)
        super().default(self, z)


class ConstantItem(object):

    def __init__(self, name):
        self._name = name
        self._data = {}

    def __lshift__(self, dict_obj):
        self._data.update(dict_obj)

    def __str__(self):
        return self.__repr__()

    def __add__(self, lhs):
        return self.__repr__() + lhs

    def data(self):
        return self._data.copy()

    def clone(self):
        ret = ConstantItem(self._name)
        ret << self._data
        return ret

    def __repr__(self):
        rendered = parms_jinja_template.render(self._data,TASK_ID=TASK_ID)
        parms = json.load(io.StringIO(str(rendered)))
        return '%s' % parms[self._name]


hafarm_defaults = {'start_frame': 1,
                   'end_frame': 48,
                   'step_frame': 1,
                   'tile_x': 1,
                   'tile_y': 1,
                   'queue': '3d',
                   'group': '',
                   'slots': 0,
                   'cpu_share': 1.0,
                   'priority': -500,
                   'req_memory': 0,
                   'req_tmpdir': 32,
                   'job_on_hold': False,
                   'hold_jid': [],
                   'hold_jid_ad': [],
                   'target_list': [],
                   'layer_list': [],
                   'command': ConstantItem('command'),
                   'command_arg': [],
                   'email_list': [],
                   'email_opt': '',
                   'make_proxy': False,
                   'job_name': ConstantItem('job_name'),
                   'log_path': '$JOB/render/sungrid/log',
                   'script_path': '$JOB/render/sungrid/jobScript',
                   'email_stdout': False,
                   'email_stderr': False,
                   'scene_file': ConstantItem('scene_file'),
                   'user': '',
                   'include_list': [],
                   'exclude_list': [],
                   'ignore_check': False,
                   'job_asset_name': '',
                   'job_asset_type': '',
                   'job_current': '',
                   'rerun_on_error': True,
                   'submission_time': 0.0,
                   'req_start_time': 0.0,
                   'req_resources': '',
                   'req_license': '',
                   'output_picture': '',
                   'frame_range_arg': ['%s%s%s',
                                       '',
                                       '',
                                       ''],
                   'frame_list': '',
                   'max_running_tasks': 1000,
                   'frame_padding_length': 4,
                   'pre_render_script': '',
                   'post_render_script': ''}
