
"""
Make test:
> rez env houdini
>> hython hafarm/tests/test_HoudiniSlurm.py

"""


from contextlib import contextmanager
import os
import shutil
import tempfile
import json
import re

from hafarm import HaGraph
from hafarm import Batch
from hafarm import HaContext
from hafarm import SlurmRender

import unittest

HAFARM_TEST_DIRECTORY = os.environ['HAFARM_HOME'] + os.sep + 'tests'

os.environ['REZ_USED_RESOLVE'] = "test_package-1.0.0"


@contextmanager
def tempdir(prefix, remove=True):
    dirpath = tempfile.mkdtemp(prefix=prefix) 
    os.environ['JOB'] = dirpath
    generated_directory = dirpath + '/render/sungrid/jobScript'
    os.makedirs(generated_directory)
    try:
        yield dirpath, generated_directory
    except:
        raise
    if remove == True:
        shutil.rmtree(dirpath)
        



regex_patterns = [
     re.compile('hafarm_slurm_test1_SlurmFiles([0-9a-z_]+)', flags=re.IGNORECASE)
    ,re.compile('hafarm/(v?\\d+.\\d+.\\d+)')
    ,re.compile('hafarm_slurm_test2_MoreOptions([0-9a-z_]+)', flags=re.IGNORECASE)
    ,re.compile('houdini/(v?\\d+.\\d+.\\d+[\\-]?\\d+)') 
    ,re.compile(os.environ['USER'])]



class TestTmpHoudiniSlurm(unittest.TestCase):
    def setUp(self):
        def c(p, t, n, s, e): 
            node = p.createNode(t, n)
            node.parm('trange').set(1)
            node.parm('f1').set(s)
            node.parm('f2').set(e)
            return node

        hou.hipFile.clear(suppress_save_prompt=True)
        hou.hipFile.save('/tmp/testRenderSlurm.hip')
        self.end_frame = 10
        n = hou.node('/out')

        self.teapot     = c(n, 'geometry', 'teapot', 1, 10)
        self.box_teapot = c(n, 'ifd', 'box_teapot', 1, 10) 
        self.alembic    = c(n, 'alembic', 'alembic', 1, 10)
        self.grid       = c(n, 'ifd', 'grid', 1, 10)
        self.comp       = c(n, 'comp', 'comp', 1, 10)
        self.root       = n.createNode("HaFarm")

        for x in [self.teapot, self.box_teapot, self.alembic, self.grid, self.comp, self.root ]:
            x.moveToGoodPosition()

        self.root.setFirstInput(self.comp)
        self.comp.setNextInput(self.box_teapot)
        self.comp.setNextInput(self.grid)
        self.box_teapot.setNextInput(self.teapot)
        self.teapot.setNextInput(self.alembic)

        self.root.parm("group").set("renders")
        self.root.parm("queue").set("cuda")
        self.root.parm("priority").set(-100)
        self.root.parm("job_on_hold").set(True)

        self.TST_DIRECTORY = HAFARM_TEST_DIRECTORY + os.sep + 'slurm_HoudiniFiles'
        self.longMessage = True
        self.maxDiff = None


    def _test_json(self, json_expected, json_actual):
        tst_body = ['inputs']
        expected, actual = {}, {}
        expected = dict( [(x, json_expected[x]) for x in tst_body] )
        actual = dict( [(x, json_actual[x]) for x in tst_body] )
        self.assertDictEqual(expected, actual, "Expected %s != %s " %( json_expected["class_name"], json_actual["class_name"]) )   

        expected, actual = {}, {}
        tst_params = [   'scene_file', 'command_arg'
                        , 'target_list', 'output_picture'
                        , 'job_name', 'command', 'priority'
                        , 'job_on_hold', 'queue', 'group'
                        , 'req_memory', 'req_resources'
                        , 'slots', 'max_running_tasks', 'req_start_time']

        def fix_jobdir(val):
            if isinstance(val, unicode):
                for pat in regex_patterns:
                    val = re.sub(pat, '_', val)
                return val
            if isinstance(val, list):
                return [ fix_jobdir(x) for x in  val]
            return val

        expected = dict( [(x, fix_jobdir(json_expected['parms'][x])) for x in tst_params] )
        actual = dict( [(x, fix_jobdir(json_actual['parms'][x])) for x in tst_params] )
        self.assertDictEqual(expected, actual, "Expected %s != %s " %( json_expected["class_name"], json_actual["class_name"]) )

        return True


    def _test_job(self, job_expected, job_actual):
        for pat in regex_patterns:
            job_expected = [re.sub(pat, '_', x) for x in job_expected if not 'HAFARM_VERSION' in x ]
            job_actual = [re.sub(pat, '_', x) for x in job_actual if not 'HAFARM_VERSION' in x ]

        self.assertListEqual(job_expected, job_actual, 'incorrect line')
        return True


    def _test_files(self, expected_files, actual_files, output_directory):
        actual_names = [os.path.split(x)[1] for x in actual_files]

        self.assertListEqual(expected_files, actual_names, 'incorrect file names')

        for filename in expected_files:
            _, ext = os.path.splitext(filename)

            if ext == '.json':
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


            if ext == '.job':
                job_actual = ''
                job_expected = ''
                with open(output_directory + os.sep + filename) as f:
                    job_actual = f.readlines()

                with open(self.TST_DIRECTORY + os.sep + filename) as f:
                    job_expected = f.readlines()
                
                try:
                    self.assertEqual(self._test_job(job_expected, job_actual), True, filename)
                except AssertionError, e:
                    print "HA ERROR: in ######## %s ############" % filename
                    print e



    def test2_MoreOptions(self):
        with tempdir('hafarm_slurm_test2_MoreOptions') as (tmp, generated_directory):
            hou.setPwd(self.root)
            self.root.parm("more").set(True)
            self.root.parm("hbatch_slots").set(44)
            self.root.parm("mantra_slots").set(55)
            self.root.parm("hbatch_ram").set(64)
            self.root.parm("mantra_ram").set(32)
            self.root.parm("max_running_tasks").set(77)
            self.root.parm("delay").set(7)

            ctx = HaContext.HaContext(external_hashes=map(lambda x: str(x).rjust(4,'Z'), xrange(1,90)))
            graph = ctx.get_graph()
            graph.set_render(SlurmRender.SlurmRender)
            json_files = graph.render(json_output_directory=generated_directory, dryrun=True)
            json_files.sort()

            self.assertEqual(len(json_files), 7, 'incorrect count files')

            json_expected_files = [  'testRenderSlurm.hip_ZZ10_grid_ifd.hip'
                                    ,'testRenderSlurm.hip_ZZ10_grid_ifd.job'
                                    ,'testRenderSlurm.hip_ZZ10_grid_ifd.json'
                                    ,'testRenderSlurm.hip_ZZ10_grid_mantra.job'
                                    ,'testRenderSlurm.hip_ZZ10_grid_mantra.json'
                                    ,'testRenderSlurm.hip_ZZ13_comp_comp.hip'
                                    ,'testRenderSlurm.hip_ZZ13_comp_comp.job'
                                    ,'testRenderSlurm.hip_ZZ13_comp_comp.json'
                                    ,'testRenderSlurm.hip_ZZZ2_alembic_alembic.hip'
                                    ,'testRenderSlurm.hip_ZZZ2_alembic_alembic.job'
                                    ,'testRenderSlurm.hip_ZZZ2_alembic_alembic.json'
                                    ,'testRenderSlurm.hip_ZZZ4_teapot_geometry.hip'
                                    ,'testRenderSlurm.hip_ZZZ4_teapot_geometry.job'
                                    ,'testRenderSlurm.hip_ZZZ4_teapot_geometry.json'
                                    ,'testRenderSlurm.hip_ZZZ6_box_teapot_ifd.hip'
                                    ,'testRenderSlurm.hip_ZZZ6_box_teapot_ifd.job'
                                    ,'testRenderSlurm.hip_ZZZ6_box_teapot_ifd.json'
                                    ,'testRenderSlurm.hip_ZZZ6_box_teapot_mantra.job'
                                    ,'testRenderSlurm.hip_ZZZ6_box_teapot_mantra.json' ]

            json_expected_files.sort()

            actual_files = os.listdir(generated_directory)
            actual_files.sort()

            self._test_files(json_expected_files, actual_files, generated_directory)


    def test1_SlurmFiles(self):
        with tempdir('hafarm_slurm_test1_SlurmFiles') as (tmp, generated_directory):
            hou.setPwd(self.root)
            ctx = HaContext.HaContext(external_hashes=map(lambda x: str(x).rjust(4,'Y'), xrange(1,90)))
            graph = ctx.get_graph()
            graph.set_render(SlurmRender.SlurmRender)
            json_files = graph.render(json_output_directory=generated_directory, dryrun=True)
            json_files.sort()

            self.assertEqual(len(json_files), 7, 'incorrect count files')

            json_expected_files = [  'testRenderSlurm.hip_YY10_grid_ifd.hip'
                                    ,'testRenderSlurm.hip_YY10_grid_ifd.job'
                                    ,'testRenderSlurm.hip_YY10_grid_ifd.json'
                                    ,'testRenderSlurm.hip_YY10_grid_mantra.job'
                                    ,'testRenderSlurm.hip_YY10_grid_mantra.json'
                                    ,'testRenderSlurm.hip_YY13_comp_comp.hip'
                                    ,'testRenderSlurm.hip_YY13_comp_comp.job'
                                    ,'testRenderSlurm.hip_YY13_comp_comp.json'
                                    ,'testRenderSlurm.hip_YYY2_alembic_alembic.hip'
                                    ,'testRenderSlurm.hip_YYY2_alembic_alembic.job'
                                    ,'testRenderSlurm.hip_YYY2_alembic_alembic.json'
                                    ,'testRenderSlurm.hip_YYY4_teapot_geometry.hip'
                                    ,'testRenderSlurm.hip_YYY4_teapot_geometry.job'
                                    ,'testRenderSlurm.hip_YYY4_teapot_geometry.json'
                                    ,'testRenderSlurm.hip_YYY6_box_teapot_ifd.hip'
                                    ,'testRenderSlurm.hip_YYY6_box_teapot_ifd.job'
                                    ,'testRenderSlurm.hip_YYY6_box_teapot_ifd.json'
                                    ,'testRenderSlurm.hip_YYY6_box_teapot_mantra.job'
                                    ,'testRenderSlurm.hip_YYY6_box_teapot_mantra.json' ]

            json_expected_files.sort()

            actual_files = os.listdir(generated_directory)
            actual_files.sort()

            self._test_files(json_expected_files, actual_files, generated_directory)


def run():
    for test in [TestTmpHoudiniSlurm]: 
         case = unittest.TestLoader().loadTestsFromTestCase(test)
         unittest.TextTestRunner(verbosity=3).run(case)
run()




