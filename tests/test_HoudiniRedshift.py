#!/usr/bin/python2.6


"""
Make test:
hython hafarm/tests/test_HoudiniRedshift.py

"""

import unittest
import sys, os, tempfile, shutil
import json
from contextlib import contextmanager


@contextmanager
def tempdir(prefix):
    os.environ['JOB'] = '/tmp'
    dirpath = tempfile.mkdtemp(prefix=prefix)
    try:
        yield dirpath
    except:
        raise
    shutil.rmtree(dirpath)

os.environ['REZ_USED_RESOLVE'] = "test_package-1.0.0"    


# FIXME: Just Can't handle it. Studio installed version breaks tests. 
# Tests with relative paths break while running cases because tested 
# objects import our modules and expects proper paths...

# Remove studio-wide installation:
try:
    index = sys.path.index("/STUDIO/studio-packages")
    sys.path.pop(index)
except:
    pass

# make ../../ha/hafarm visible for tests:
sys.path.insert(0, os.path.join(os.getcwd(), "../.."))

def enableHouModule():
    '''Set up the environment so that "import hou" works.'''
    import sys, os

    # Importing hou will load in Houdini's libraries and initialize Houdini.
    # In turn, Houdini will load any HDK extensions written in C++.  These
    # extensions need to link against Houdini's libraries, so we need to
    # make sure that the symbols from Houdini's libraries are visible to
    # other libraries that Houdini loads.  So, we adjust Python's dlopen
    # flags before importing hou.
    if hasattr(sys, "setdlopenflags"):
        old_dlopen_flags = sys.getdlopenflags()
        import DLFCN
        sys.setdlopenflags(old_dlopen_flags | DLFCN.RTLD_GLOBAL)

    try:
        import hou
    except ImportError:
        # Add $HFS/houdini/python2.6libs to sys.path so Python can find the
        # hou module.
        sys.path.append(os.environ['HFS'] + "/houdini/python%d.%dlibs" % sys.version_info[:2])
        import hou
    finally:
        if hasattr(sys, "setdlopenflags"):
            sys.setdlopenflags(old_dlopen_flags)

enableHouModule()

try:
    import hou
except ImportError:
    print "Tests have to be run in presence of Houdini python module or by hython."
    sys.exit()

import hafarm
from hafarm import Houdini
from hafarm import Batch
from hafarm import const
from hafarm import utils
from hafarm import HaContext
from hafarm import PrintRender
from hafarm import GraphvizRender

HAFARM_TEST_DIRECTORY = os.environ['HAFARM_HOME'] + os.sep + 'tests'


class TestRenderPressed(unittest.TestCase):
    def setUp(self):
        def c(p, t, n, s, e): 
            node = p.createNode(t, n)
            node.parm('trange').set(1)
            node.parm('f1').set(s)
            node.parm('f2').set(e)
            return node

        hou.hipFile.clear(suppress_save_prompt=True)
        hou.hipFile.save('/tmp/testRedshiftRenderPressed.hip')
        self.end_frame = 10
        n = hou.node('/out')

        self.teapot     = c(n, 'geometry', 'teapot', 1, 10)
        self.teapot_rs = c(n, 'Redshift_ROP', 'teapotRS', 1, 10) 
        self.root       = n.createNode("HaFarm")

        for x in [self.teapot, self.teapot_rs, self.root ]:
            x.moveToGoodPosition()

        self.root.setFirstInput(self.teapot_rs)
        self.teapot_rs.setNextInput(self.teapot)
        self.root.parm("group").set("cuda")

        self.TST_DIRECTORY = HAFARM_TEST_DIRECTORY + os.sep + 'houdiniRS_TestRenderPressed'
        self.longMessage = True
        self.maxDiff = None


    def _test_json(self, json_expected, json_actual):
        tst_body = ['inputs']
        expected, actual = {}, {}
        expected = dict( [(x, json_expected[x]) for x in tst_body] )
        actual = dict( [(x, json_actual[x]) for x in tst_body] )
        self.assertDictEqual(expected, actual, "Expected %s != %s " %( json_expected["class_name"],  json_actual["class_name"]) )   
        
        expected, actual = {}, {}
        tst_params = ['scene_file', 'command_arg', 'target_list', 'output_picture', 'job_name', 'command'] 
        expected = dict( [(x, json_expected['parms'][x]) for x in tst_params] )
        actual = dict( [(x, json_actual['parms'][x]) for x in tst_params] )
        self.assertDictEqual(expected, actual, "Expected %s != %s " %( json_expected["class_name"],  json_actual["class_name"]))

        return True


    def _test_files(self, json_expected_files, json_files):
        json_actual_names = [os.path.split(x)[1] for x in json_files]

        self.assertListEqual(json_expected_files, json_actual_names, 'incorrect file names')

        output_directory = os.path.split(json_files[0])[0]

        for filename in json_expected_files:
            json_actual = {}
            json_expected = {}

            with open(output_directory + os.sep + filename) as f:
                json_actual = json.load(f)

            with open(self.TST_DIRECTORY + os.sep + filename ) as f:
                json_expected = json.load(f)
            
            try:
                self.assertEqual(self._test_json(json_expected, json_actual), True, filename)
            except AssertionError, e:
                    print "HA ERROR: in ######## %s ############" % filename
                    print e



    def test_Hafarm1(self):
        with tempdir('hafarm_houRS_test_Hafarm1') as tmppath:
            hou.setPwd(self.root)
            ctx = HaContext.HaContext(external_hashes=map(lambda x: str(x).rjust(4,'Y'), xrange(1,90)))
            graph = ctx.get_graph()
            json_files = graph.render(json_output_directory=tmppath, copy_scene_file=True)
            json_files.sort()

            self.assertEqual(len(json_files), 3, 'incorrect count files')

            json_expected_files = [  'testRedshiftRenderPressed.hip_YYY2_teapot_geometry.json'
                                    ,'testRedshiftRenderPressed.hip_YYY4_teapotRS_redshift.json'
                                    ,'testRedshiftRenderPressed.hip_YYY4_teapotRS_rs.json' ]

            json_expected_files.sort()
            self._test_files(json_expected_files, json_files)

        return True



#if __name__ == '__main__':
def run():
    for test in [TestRenderPressed]: # TestMantraRenderFromIfd, TestRenderPressed, TestMantraRenderWithTilesRenderPressed
         case = unittest.TestLoader().loadTestsFromTestCase(test)
         unittest.TextTestRunner(verbosity=3).run(case)
run()