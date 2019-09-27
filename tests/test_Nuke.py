#!/usr/bin/python2.6
import unittest
import sys, os, tempfile, shutil
import json
from contextlib import contextmanager

import nuke

"""
Make test:
nuke -t hafarm/tests/test_Nuke.py

"""

@contextmanager
def tempdir(prefix):
    dirpath = tempfile.mkdtemp(prefix=prefix)
    os.environ['JOB'] = '/tmp'
    try:
        yield dirpath
    except:
        raise
    shutil.rmtree(dirpath)



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

import hafarm
from hafarm import Nuke
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

        constant1 = nuke.createNode('Constant')
        self.write1 = nuke.createNode('Write')
        self.write1.setInput(0, constant1)

        nuke.scriptSaveAs('/tmp/testRenderPressed.nk',1)
        # nuke.root().setName('testRenderPressed.nk')

        self.TST_DIRECTORY = HAFARM_TEST_DIRECTORY + os.sep + 'nuke_TestRenderPressed'
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

        for n in json_expected_files:
            json_actual = {}
            json_expected = {}

            with open(output_directory + os.sep + n) as f:
                json_actual = json.load(f)

            with open(self.TST_DIRECTORY + os.sep + n ) as f:
                json_expected = json.load(f)

            self.assertEqual(self._test_json(json_expected, json_actual), True, n)

    
    def test_Hafarm1(self):
        with tempdir('hafarm_nuke_test_Hafarm1') as tmppath:
            nuke.root().setSelected(self.write1)
            ctx = HaContext.HaContext(external_hashes=map(lambda x: str(x).rjust(4,'Y'), xrange(1,90)))

            kwargs = dict(
                     queue = 'nuke'
                    ,group = 'render'
                    ,start_frame = 1
                    ,end_frame = 100
                    ,frame_range = 1
                    ,job_on_hold = False
                    ,priority = 100
                )

            graph = ctx.get_graph(**kwargs)
            json_files = graph.render(json_output_directory=tmppath, copy_scene_file=True)
            json_files.sort()

            self.assertEqual(len(json_files), 1, 'incorrect count files')

            json_expected_files = [ 'testRenderPressed.nk_YYY1_Write1.json' ]

            json_expected_files.sort()
            self._test_files(json_expected_files, json_files)

        return True


def run():
    for test in [TestRenderPressed]: 
         case = unittest.TestLoader().loadTestsFromTestCase(test)
         unittest.TextTestRunner(verbosity=3).run(case)
run()