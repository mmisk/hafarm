import os
import json
import pprint
from time import time
from hafarm import const
from const import ConstantItemJSONEncoder

class PrintParmRender(object):

    def __init__(self, hagraphitems_lst, **kwargs):
        super(PrintParmRender, self).__init__()
        self.hagraphitems_lst = hagraphitems_lst

    def render(self, **kwargs):
        print
        print
        for k, n in self.hagraphitems_lst.iteritems():
            pprint.pprint(n.parms)


class JsonParmRender(object):

    def __init__(self, hagraphitems_lst, **kwargs):
        super(JsonParmRender, self).__init__()
        self.hagraphitems_lst = hagraphitems_lst

    def render(self, **kwargs):
        files = {}
        for k, item in self.hagraphitems_lst.iteritems():
            item.parms['submission_time'] = time()
            _db = {}
            _db['inputs'] = [ self.hagraphitems_lst[x].parms['job_name'] for x in item.dependencies ]
            _db['class_name'] = item.__class__.__name__
            _db['backend_name'] = 'JsonParmRender'
            _db['parms'] = item.parms
            parms_file = os.path.expandvars(str(item.parms['script_path']))
            parms_file = os.path.join(parms_file, str(item.parms['job_name'])) + '.json'
            with open(parms_file, 'w') as file:
                result = json.dump(_db, file, indent=2, cls=ConstantItemJSONEncoder)
            files[k] = parms_file

        return files
