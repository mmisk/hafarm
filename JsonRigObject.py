import os
import json



class _JsonRigObject100(object):
    def __init__(self, jsonfilepath):
        self._latest = {}
        self._extensions = []
        self.force = False
        with open(jsonfilepath) as json_file:
            parms = json.load(json_file)
            self._latest = parms['latest']

            for ext in self._latest:
                json_filepath = self._latest[ext]
                if not os.path.exists(json_filepath):
                    print "[  HA ERROR  ] Path '%s' not found. Skipped..." % json_filepath
                    continue
                self._extensions += [ext]
                with open(json_filepath) as json_file:
                    self._latest[ext] = json.load(json_file)


    @staticmethod
    def create(maya_scene_path):
        path, _ = os.path.split(maya_scene_path)
        jsrig = _JsonRigObject100(path + os.sep + 'rig.json')
        return jsrig


    def extension(self, short_name):
        for ext in self._latest:
            for set_obj in self._latest[ext]['export_sets']:
                if set_obj['short_name'] == short_name:
                    return ext
        return None


    def export_sets(self, extension):
        if extension in self._latest:
            return dict( (x['short_name'], x['long_name']) for x in self._latest[extension]['export_sets'] )
        else:
            # TODO:
            keys = self._latest.keys()
            if keys == []:
                raise Exception('[  HA ERROR  ] No extensions in ')
            print '[ HA WARNING ] No "%s" extension. Use "%s" ' % (extension, keys[0])
            self.force = True
            return dict( (x['short_name'], x['long_name']) for x in self._latest[keys[0]]['export_sets'] )


    def all_export_sets(self):
        ret = {}
        for ext in self._extensions:
            ret.update( self.export_sets(ext) )
        return ret

    def __str__(self):
        return "JsonRigObject:: %s" % self._latest




class _JsonRigObject110(object):
    _resolution_priorities = {0:'low', 1:'mid', 2: 'high'}

    def __init__(self, jsonfilepath):
        self._latest = {}
        self._variants = {}
        self._versions = {}
        self._resolutions = []
        self._asset_name = ''
        self._top_resolution = -100
        self.force = False
        self._paths = {}
        self._collection = {}
        
        with open(jsonfilepath) as json_file:
            parms = json.load(json_file)
            self._latest = parms['latest']
            self._asset_name = parms['job_asset_name']

            _res = dict( (y,x) for x,y in self._resolution_priorities.iteritems()  )

            for var in self._latest:
                _resolutions = {}
                _versions = {}
                for ver in self._latest[var]:
                    for res in self._latest[var][ver]:
                        if self._top_resolution < _res[res]:
                            self._top_resolution = _res[res]

                        self._resolutions += [ res ]
                        json_filepath = self._latest[var][ver][res]
                        if not os.path.exists(json_filepath):
                            print "[  HA ERROR  ] Path '%s' not found. Skipped..." % json_filepath
                            continue
                        with open(json_filepath) as json_file:
                            _resolutions[res] = json.load(json_file)
                            self._paths[res] = _resolutions[res]['path']
                    _versions[ver] = _resolutions
                self._variants[var] = _versions


    @classmethod
    def resolution_priorities(cls):
        return cls._resolution_priorities


    def asset_name(self):
        return self._asset_name

    @staticmethod
    def create(maya_scene_path):
        name = str(maya_scene_path).rsplit(os.sep,3)[-3]
        path, _ = os.path.split(maya_scene_path)
        jsrig = _JsonRigObject110(path + os.sep + name + '.json')

        return jsrig



    def path(self, short_name):
        res = self.resolution(short_name)
        # TODO: if short_name from another asset ? It is happens when reference_hires.py converts path
        return self._paths.get(res,"")


    def variant(self, short_name):
        for var in self._variants:
            for ver in self._latest[var]:
                for res in self._variants[var][ver]:
                    for set_obj in self._variants[var][ver][res]['export_sets']:
                        if set_obj['short_name'] == short_name:
                            return var
        return None


    def top_resolution(self, short_name):
        ret = self.resolution(short_name)
        if ret == None:
            ret = self._resolution_priorities[self._top_resolution]
        return ret


    def resolution(self, short_name):
        for var in self._variants:
            for ver in self._latest[var]:
                for res in self._variants[var][ver]:
                    for set_obj in self._variants[var][ver][res]['export_sets']:
                        if set_obj['short_name'] == short_name:
                            return res
        return None #self._resolution_priorities[self._top_resolution]


    def export_sets(self, resolution):
        if resolution in self._resolutions:
            ret = {}
            for var in self._variants:
                for ver in self._latest[var]:
                    for res in self._variants[var][ver]:
                        if res == resolution:
                            ret.update( dict( (x['short_name'], x['long_name']) for x in self._variants[var][ver][res]['export_sets'] ) )
            return ret
        else:
            # TODO:
            res = self._resolution_priorities[self._top_resolution]
            print '[ HA WARNING ] No "%s" resolution. Use "%s" ' % (resolution, res)
            # TODO: refactor next line
            self.force = True
            return self.export_sets(res)


    def all_export_sets(self):
        ret = {}
        for var in self._variants:
            for ver in self._latest[var]:
                for res in self._variants[var][ver]:
                    for set_obj in self._variants[var][ver][res]['export_sets']:
                        ret.update( self.export_sets(res) )
        return ret

    def all_export_sets2(self):
        ret = {}
        for var in self._variants:
            for ver in self._latest[var]:
                for res in self._variants[var][ver]:
                    for set_obj in self._variants[var][ver][res]['export_sets']:
                        ret.update( self.export_sets(res) )
        return ret


    def __iter__(self):
        # For tests only, yet
        for n,m in self._paths.iteritems():
            yield n,m

    def __str__(self):
        return "JsonRigObject:: %s" % self._variants       


class _JsonRigObjectDummy(object):
    force = False
    def extension(self, val):
        return None
    def export_sets(self, val):
        return {}
    def all_export_sets(self):
        return {}
    def path(self, val):
        return ''


class JsonRigObjectFactory(type):
    def __call__(cls, json_scene_path):
        try:
            return _JsonRigObject110(maya_scene_path)
        except Exception, e:
            print "[ HA WARNING ] Try to use 1.0.0", e
            return _JsonRigObject100(maya_scene_path)


class JsonRigObject(object):
    __metaclass__ = JsonRigObjectFactory 
    
    @staticmethod
    def create(maya_scene_path):
        try:
            return _JsonRigObject110.create(maya_scene_path)
        except Exception, e:
            print "[ HA WARNING ] Try to use 1.0.0", e
        
        try:
            return _JsonRigObject100.create(maya_scene_path)
        except Exception, e:
            print "[ HA WARNING ] Skip", maya_scene_path, e

        return _JsonRigObjectDummy()
