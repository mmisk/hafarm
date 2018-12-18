
"""
Make test:
python hafarm/tests/send_to_slurm.py

"""


from contextlib import contextmanager
import os
import shutil
import tempfile
import json

from hafarm import HaGraph
from hafarm import Batch
from hafarm import SlurmRender

import unittest

from hafarm.HaGraph import HaGraphItem
HaGraphItem._external_hashes = (lambda: [(yield x) for x in map(lambda x: str(x).rjust(4,'X'), xrange(1,90)) ])()

HAFARM_TEST_DIRECTORY = os.environ['HAFARM_HOME'] + os.sep + 'tests'

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


class TestTmpSlurm(unittest.TestCase):
    def setUp(self):
        self.TST_DIRECTORY = HAFARM_TEST_DIRECTORY + os.sep + 'slurm_TestFiles'
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


    def _test_job(self, job_expected, job_actual):
        self.assertListEqual(job_expected, job_actual, 'incorrect line')
        return True


    def _test_files(self, expected_files, actual_files, output_directory):
        actual_names = [os.path.split(x)[1] for x in actual_files]

        self.assertListEqual(expected_files, actual_names, 'incorrect file names')

        for n in expected_files:
            _, ext = os.path.splitext(n)

            if ext == '.json':
                json_actual = {}
                json_expected = {}
                with open(output_directory + os.sep + n) as f:
                    json_actual = json.load(f)

                with open(self.TST_DIRECTORY + os.sep + n ) as f:
                    json_expected = json.load(f)

                self.assertEqual(self._test_json(json_expected, json_actual), True, n)

            if ext == '.job':
                job_actual = ''
                job_expected = ''
                with open(output_directory + os.sep + n) as f:
                    job_actual = f.readlines()

                with open(self.TST_DIRECTORY + os.sep + n ) as f:
                    job_expected = f.readlines()

                self.assertEqual(self._test_job(job_expected, job_actual), True, n)


    def test1_replaceTASKID(self):
        with tempdir('hafarm_slurm_test1_replaceTASKID') as (tmp, generated_directory):
            item1 = Batch.BatchBase("testR",'/hafarm/test_replace1')
            path = '/tmp'
            ext = '.ifd'

            item1.parms['exe'] = "FILE_FOR_DIFF "

            item1.parms['scene_file'] << { 'scene_file_path': path
                                            , 'scene_file_basename':  'test1.hip' 
                                            , 'scene_file_ext': ext 
                                            , 'scene_file_hash': 'YYY2'
                                          }

            graph = HaGraph.HaGraph(graph_items_args=[])

            graph.add_node(item1)

            graph.set_render(SlurmRender.SlurmRender)
            json_files = graph.render(dryrun=True)

            self.assertEqual(len(json_files), 1, 'incorrect count files')
            
            expected_files = [   'testR_XXX1__bs.job'
                                ,'testR_XXX1__bs.json' ]

            actual_files = os.listdir(generated_directory)
            actual_files.sort()

            self._test_files(expected_files, actual_files, generated_directory)


    def test2_GeneratedFiles(self):
        with tempdir('hafarm_slurm_test2_GeneratedFiles') as (tmp, generated_directory):
            item1 = Batch.BatchBase("test1",'/hafarm/test1')
            item1.parms['scene_file'] << { 'scene_fullpath': '/tmp/test1' }
            item1.parms['command_arg'] = ["-p"]
            item1.parms['exe'] = "mkdir"

            item2 = Batch.BatchBase("test2",'/hafarm/test2')
            item2.parms['scene_file'] << { 'scene_fullpath': '/tmp/test2' }
            item2.parms['exe'] = "touch"
            item2.add( item1 )

            item3 = Batch.BatchBase("test3",'/hafarm/test3')
            item3.parms['scene_file'] << { 'scene_fullpath':  '/tmp/test1/test2' }
            item3.parms['command_arg'] = ["item3", ">>"]
            item3.parms['exe'] = "echo"
            item3.add( item1, item2 )

            graph = HaGraph.HaGraph(graph_items_args=[])

            graph.add_node(item1)
            graph.add_node(item2)
            graph.add_node(item3)

            graph.set_render(SlurmRender.SlurmRender)
            json_files = graph.render(dryrun=True)

            self.assertEqual(len(json_files), 3, 'incorrect count files')

            expected_files = [   'test1_XXX2__bs.job'
                                ,'test1_XXX2__bs.json'
                                ,'test2_XXX3__bs.job'
                                ,'test2_XXX3__bs.json'
                                ,'test3_XXX4__bs.job'
                                ,'test3_XXX4__bs.json' ]

            actual_files = os.listdir(generated_directory)
            actual_files.sort()

            self._test_files(expected_files, actual_files, generated_directory)


    def test3_change_command(self):
        with tempdir('hafarm_slurm_test3_ChangeCommand') as (tmp, generated_directory):
            item1 = Batch.BatchBase("test4Cmd",'/hafarm/test1')
            item1.parms['scene_file'] << { 'scene_fullpath': '/tmp/test1' }
            item1.parms['output_picture'] = "/tmp/test1.exr"
            item1.parms['command_arg'] = ["this", "list", "is", "not", "to", "be", "in", "command_arg"]
            item1.parms['command'] << "{exe} {scene_file} {output_picture}"
            item1.parms['exe'] = "ffmpeg"

            graph = HaGraph.HaGraph(graph_items_args=[])
            graph.add_node(item1)
            graph.set_render(SlurmRender.SlurmRender)
            json_files = graph.render(dryrun=True)
            
            self.assertEqual(len(json_files), 1, 'incorrect count files')
            
            expected_files = [   'test4Cmd_XXX5__bs.job'
                                ,'test4Cmd_XXX5__bs.json' ]

            actual_files = os.listdir(generated_directory)
            actual_files.sort()

            self._test_files(expected_files, actual_files, generated_directory)



def run():
    for test in [TestTmpSlurm]: 
         case = unittest.TestLoader().loadTestsFromTestCase(test)
         unittest.TextTestRunner(verbosity=3).run(case)
run()




