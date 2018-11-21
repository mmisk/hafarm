import os
import re
from hafarm.const import ConstantItem


item = ConstantItem('command')

item << { 'command': "$USER/workspace" }

str_obj = "$USER/workspace"


_varprog = re.compile(r'\$(\w+|\{[^}]*\})')
i = 0
while True:
    m = _varprog.search(item, i)
    print m
    if not m:
        break
    i, j = m.span(0)
    name = m.group(1)
    if name.startswith('{') and name.endswith('}'):
        name = name[1:-1]
    if name in os.environ:
        tail = path[j:]
        path = path[:i] + os.environ[name]
        i = len(path)
        path += tail
    else:
        i = j


print os.path.expandvars(str_obj)
print os.path.expandvars(item)