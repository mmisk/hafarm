import os
import re
import hafarm.const
from hafarm.const import ConstantItem


command = ConstantItem('command')
command << { 'command': "$USER/workspace" }

str_obj = "$USER/workspace"

job_name = ConstantItem('job_name')
job_name << { 'jobname_hash' : 'X1xx' }
job_name << { 'render_driver_name' : 'ROP1' }
job_name << { 'render_driver_type' : 'ifd' }

print job_name
assert str(job_name) == 'no_name_job_X1xx_ROP1_ifd'

job_name << { 'job_basename' : 'test1.hip' }

print job_name
assert str(job_name) == 'test1.hip_X1xx_ROP1_ifd'



scene_file = ConstantItem('scene_file')

path = '/tmp'
ext = '.ifd'
scene_file << { 'scene_file_path': path
				, 'scene_file_basename':  'test1.hip' 
				, 'scene_file_ext': ext 
				, 'scene_file_hash': 'XXX2'
				}
print scene_file
assert str(scene_file) == '/tmp/test1.hip_XXX2.@TASK_ID/>.ifd'


