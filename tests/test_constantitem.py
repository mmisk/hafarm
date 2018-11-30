import os
import re
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


