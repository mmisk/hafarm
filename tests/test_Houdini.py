#!/usr/bin/python2.6


"""
Make test:
hython hafarm/tests/test_Houdini.py

"""

import unittest
import sys, os, tempfile, shutil
import json
import re
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

pat1 = re.compile('hafarm_slurm_test1_SlurmFiles([0-9a-z_]+)', flags=re.IGNORECASE)
pat2 = re.compile('hafarm/(v?\\d+.\\d+.\\d+)')

class TestMantraRenderFrameList(unittest.TestCase):
    def setUp(self):
        hou.hipFile.clear()
        self.nt     = hou.node('/out')
        self.rop    = self.nt.createNode("ifd")
        self.farm   = self.nt.createNode("HaFarm")
        self.farm.setFirstInput(self.rop)
        self.end_frame = 100
        self.rop.parm('trange').set(1)
        self.rop.parm('f2').deleteAllKeyframes()
        self.rop.parm('f2').set(self.end_frame)
        self.farm.parm('use_frame_list').set(1)
        self.farm.parm('frame_list').set("1,3,5")
        self.hbatch_farm = Houdini.HbatchFarm(self.farm, self.rop)


    def test_mantra_render_frame_list(self):
        hou.setPwd(self.farm)
        ctx = HaContext.HaContext()
        graph = ctx.get_graph()

        frames = [1,3,5]
        mframes = Houdini.mantra_render_frame_list(self.hbatch_farm, frames)
        self.assertEqual(mframes, self.hbatch_farm.get_direct_outputs())
        self.assertEqual(len(frames), len(mframes))
        self.assertTrue("-l 1,3,5" in self.hbatch_farm.parms['command_arg'])
        for frame in range(len(frames)):
            self.assertEqual(mframes[frame].parms['start_frame'], frames[frame])
            self.assertEqual(mframes[frame].parms['end_frame'], frames[frame])
            self.assertEqual(mframes[frame].get_direct_inputs()[0], self.hbatch_farm)



class TestMantraRenderFromIfd(unittest.TestCase):
    def setUp(self):
        hou.hipFile.clear()
        self.nt     = hou.node('/out')
        self.rop    = self.nt.createNode("ifd")
        self.farm   = self.nt.createNode("HaFarm")
        self.farm.setFirstInput(self.rop)

        # Basic scene:
        obj = hou.node("/obj")
        geo = obj.createNode("geo")
        geo.node("file1").destroy()
        cam = obj.createNode("cam")
        grid= geo.createNode("grid")
        cam.parmTuple("t").set((5,5,5))
        cam.parmTuple("r").set((-45,45,0))

        # Create tmp ifds:
        self.tmppath   = tempfile.mkdtemp()
        self.ifdfile   = os.path.join(self.tmppath, "tmp.$F.ifd")
        self.vm_picture = os.path.join(self.tmppath, "image.$F4.exr")
        self.rop.parm('soho_outputmode').set(1)
        self.rop.parm('soho_diskfile').set(self.ifdfile)
        self.rop.parm("trange").set(1)
        self.rop.parm("f2").deleteAllKeyframes()
        self.rop.parm("f2").set(10)
        self.rop.parm("vm_picture").set(self.vm_picture)
        self.rop.render()

        # Set parms on HaFarm ROP:
        self.farm.parm("ifd_range1").set(1)
        self.farm.parm("ifd_range2").set(10)
        self.farm.parm("ifd_range3").set(1)
        self.farm.parm("ifd_files").set(self.ifdfile)

        
    def test_mantra_render_from_ifd_1(self):
        frames = None
        job_name = None
        image = os.path.join(self.tmppath, "image.0001.exr")
        self.farm.parm("render_from_ifd").set(True)
        hou.setPwd(self.farm)
        ctx = HaContext.HaContext()
        graph = ctx.get_graph()
        mantras = filter(lambda x: isinstance(x, Houdini.HoudiniMantraExistingIfdWrapper), graph.graph_items)
        for mantra in mantras:
            self.assertEqual(mantra.parms['start_frame'], self.farm.parm("ifd_range1").eval())
            self.assertEqual(mantra.parms['end_frame'],   self.farm.parm("ifd_range2").eval())
            self.assertEqual(mantra.parms['step_frame'],  self.farm.parm("ifd_range3").eval())
            self.assertTrue(const.TASK_ID  in mantra.parms['scene_file'])
        self.assertEqual(mantra.parms['output_picture'],  image)
        self.farm.parm("render_from_ifd").set(False)


    def test_mantra_render_frames(self):
        self.farm.parm("use_frame_list").set(True)
        self.farm.parm("frame_list").set('1,3,5')
        image = os.path.join(self.tmppath, "image.0001.exr")

        hou.setPwd(self.farm)
        ctx = HaContext.HaContext()
        graph = ctx.get_graph()

        frames = [1,3,5]
        mantras = filter(lambda x: isinstance(x, Houdini.HoudiniMantraExistingIfdWrapper), graph.graph_items)

        for frame in frames:
            idx = frames.index(frame)
            self.assertEqual(mantras[idx].parms['start_frame'], frame)
            self.assertEqual(mantras[idx].parms['end_frame'], frame)
            self.assertTrue(const.TASK_ID  in mantras[idx].parms['scene_file'])
            self.assertEqual(mantras[idx].parms['output_picture'],  image)


    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmppath)



class TestPostRenderActions(unittest.TestCase):
    def setUp(self):
        hou.hipFile.clear()
        self.nt     = hou.node('/out')
        self.rop    = self.nt.createNode("ifd")
        self.rop.setName('test')
        self.rop.parm("vm_picture").set("$JOB/render/mantra/images/$USER/$OS.$F4.exr")
        self.farm   = self.nt.createNode("HaFarm")
        self.farm.setFirstInput(self.rop)

        self.farm.parm("debug_images").set(1)
        self.farm.parm("make_proxy").set(1)
        self.farm.parm("make_movie").set(1)

        self.end_frame = 10
        self.rop.parm('trange').set(1)
        self.rop.parm('f2').deleteAllKeyframes()
        self.rop.parm('f2').set(self.end_frame)


    def test_post_render_actions(self):
        hou.setPwd(self.farm)
        ctx = HaContext.HaContext()
        graph = ctx.get_graph()

        self.assertTrue(len(graph.graph_items) == 5)

        # for n in graph.graph_items:
        #     print type(n), n, n.dependencies

        graph.set_render(GraphvizRender.GraphvizRender)
        graph.render()
        # items = graph.tree()

        # m,x,y,z,w = graph.graph_items

        # test_tree = {  Houdini.HoudiniMantraIFDWrapper:
        #                [
        #                     { Houdini.HoudiniMantraWrapper:
        #                         [
        #                             { Batch.BatchMp4: [] }
        #                           , { Batch.BatchDebug:
        #                             [
        #                                 { Batch.BatchReportsMerger: [] }
        #                             ] }
        #                         ]
        #                     }
        #                ]
        #             }
        
        # def _test_unpack_dict(_item):
        #     ret = {}
        #     for x in _item:
        #         ret[x] = [_test_unpack_dict(y) for y in _item[x]]
        #     return ret

        # _unpacked = _test_unpack_dict(items)

        # print _unpacked

        # self.assertTrue( _unpacked == test_tree )



        # merger dependent 

        # self.hbatch_farm = Houdini.HbatchFarm(self.farm, self.rop)
        # job_name         = self.hbatch_farm.parms['job_name'] + "_mantra"
        # self.mantra_farm = Houdini.MantraFarm(self.farm, self.rop, job_name)

        # posts = Houdini.post_render_actions(self.farm, [self.hbatch_farm, self.mantra_farm], '3d')
        # self.assertTrue(len(posts), 3)
        # debuger, merger, moviem = posts

        # # mp4:
        # image       = utils.padding(self.rop.parm('vm_picture').eval(), 'nuke')[0]
        # base, ext   = os.path.splitext(image)
        # path, file  = os.path.split(base)
        # path        = os.path.join(path,  const.PROXY_POSTFIX)
        # proxy       = os.path.join(path, file +'.jpg')
    
        # self.assertTrue(proxy in moviem.parms['command_arg'][0])
        # self.assertEqual('ffmpeg ', moviem.parms['command'])
        # self.assertTrue(moviem.parms['start_frame'] == moviem.parms['end_frame'] == 1)

        # # merger
        # image      = utils.padding(self.rop.parm('vm_picture').eval(), 'shell')[0]
        # path, file = os.path.split(image)
        # path       = os.path.join(path, const.DEBUG_POSTFIX)
        # report     = os.path.join(path, file + '.json')

        # self.assertEqual(report, merger.parms['scene_file'])
        # self.assertTrue('$HAFARM_HOME/scripts/generate_render_report.py' in merger.parms['command'])
        # self.assertTrue(merger.parms['start_frame'] == merger.parms['end_frame'] == 1)
               
        # #debuger
        # self.assertTrue('$HAFARM_HOME/scripts/debug_images.py' in debuger.parms['command'])
        # self.assertTrue(const.TASK_ID_PADDED in debuger.parms['scene_file'])
        # self.assertEqual(debuger.parms['start_frame'], self.rop.parm('f1').eval())
        # self.assertEqual(debuger.parms['end_frame'], self.rop.parm('f2').eval())



class TestMantraRenderWithTilesRenderPressed(unittest.TestCase):
    def setUp(self):
        def c(p, t, n, s, e): 
            node = p.createNode(t, n)
            node.parm('trange').set(1)
            node.parm('f1').set(s)
            node.parm('f2').set(e)
            return node

        hou.hipFile.clear(suppress_save_prompt=True)
        self.end_frame = 10
        n = hou.node('/out')

        self.teapot     = c(n, 'geometry', 'teapot', 1, 10)
        self.box        = c(n, 'geometry', 'box', 1, 1)  
        self.box_teapot = c(n, 'ifd', 'box_teapot', 1, 10) 
        self.alembic    = c(n, 'alembic', 'alembic', 1, 10)
        self.grid       = c(n, 'ifd', 'grid', 1, 10)
        self.hafarm1    = n.createNode('HaFarm')
        self.comp       = c(n, 'comp', 'comp', 1, 10)
        self.root       = n.createNode("HaFarm")

        for x in [self.teapot, self.box, self.box_teapot, self.alembic, self.grid, self.hafarm1 , self.comp, self.root ]:
            x.moveToGoodPosition()

        self.root.parm("debug_graph").set(1)
        self.root.setFirstInput(self.comp)
        self.comp.setNextInput(self.box_teapot)
        self.comp.setNextInput(self.hafarm1)
        self.box_teapot.setNextInput(self.teapot)
        self.box_teapot.setNextInput(self.box)
        self.hafarm1.setNextInput(self.grid)
        self.grid.setNextInput(self.box)
        self.grid.setNextInput(self.alembic)
        self.hafarm1.parm("group").set("renders")


    def test_Hafarm1(self):
        self.root.parm("make_proxy").set(True)
        self.root.parm("make_movie").set(True)
        self.root.parm("debug_images").set(True)
        self.grid.parm("vm_tile_render").set(True)
        hou.setPwd(self.root)
        ctx = HaContext.HaContext()
        graph = ctx.get_graph()
        graph.set_render(PrintRender.JsonParmRender)
        json_files = graph.render()
        return True



class TestRenderPressed(unittest.TestCase):
    def setUp(self):
        def c(p, t, n, s, e): 
            node = p.createNode(t, n)
            node.parm('trange').set(1)
            node.parm('f1').set(s)
            node.parm('f2').set(e)
            return node

        hou.hipFile.clear(suppress_save_prompt=True)
        hou.hipFile.save('/tmp/testRenderPressed.hip')
        self.end_frame = 10
        n = hou.node('/out')

        self.teapot     = c(n, 'geometry', 'teapot', 1, 10)
        self.box        = c(n, 'geometry', 'box', 1, 1)  
        self.box_teapot = c(n, 'ifd', 'box_teapot', 1, 10) 
        self.alembic    = c(n, 'alembic', 'alembic', 1, 10)
        self.grid       = c(n, 'ifd', 'grid', 1, 10)
        self.hafarm1    = n.createNode('HaFarm')
        self.comp       = c(n, 'comp', 'comp', 1, 10)
        self.root       = n.createNode("HaFarm")

        for x in [self.teapot, self.box, self.box_teapot, self.alembic, self.grid, self.hafarm1 , self.comp, self.root ]:
            x.moveToGoodPosition()

        self.root.parm("debug_graph").set(1)
        self.root.setFirstInput(self.comp)
        self.comp.setNextInput(self.box_teapot)
        self.comp.setNextInput(self.hafarm1)
        self.box_teapot.setNextInput(self.teapot)
        self.box_teapot.setNextInput(self.box)
        self.hafarm1.setNextInput(self.grid)
        self.grid.setNextInput(self.box)
        self.grid.setNextInput(self.alembic)
        self.hafarm1.parm("group").set("renders")

        self.TST_DIRECTORY = HAFARM_TEST_DIRECTORY + os.sep + 'houdini_TestRenderPressed'
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
        
        def fix_jobdir(val):
            if isinstance(val, unicode):
                val = re.sub(pat1, '_', val)
                return re.sub(pat2, '_', val)
            if isinstance(val, list):
                return [ fix_jobdir(x) for x in  val]
            return val

        expected = dict( [(x, fix_jobdir(json_expected['parms'][x])) for x in tst_params] )
        actual = dict( [(x, fix_jobdir(json_actual['parms'][x])) for x in tst_params] )
        self.assertDictEqual(expected, actual, "Expected %s != %s " %( json_expected["class_name"], json_actual["class_name"]) )

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
        with tempdir('hafarm_hou_test_Hafarm1') as tmppath:
            hou.setPwd(self.hafarm1)
            ctx = HaContext.HaContext(external_hashes=map(lambda x: str(x).rjust(4,'Y'), xrange(1,90)))
            graph = ctx.get_graph()
            json_files = graph.render(json_output_directory=tmppath, copy_scene_file=True)
            json_files.sort()

            self.assertEqual(len(json_files), 4, 'incorrect count files')

            json_expected_files = [ 'testRenderPressed.hip_YYY2_box_geometry.json'
                                    ,'testRenderPressed.hip_YYY4_alembic_alembic.json'
                                    ,'testRenderPressed.hip_YYY6_grid_ifd.json'
                                    ,'testRenderPressed.hip_YYY6_grid_mantra.json' ]

            json_expected_files.sort()
            self._test_files(json_expected_files, json_files)

        return True


    def test_Hafarm2(self):
        with tempdir('hafarm_hou_test_Hafarm2') as tmppath:
            self.root.parm("make_proxy").set(True)
            self.root.parm("make_movie").set(True)
            self.root.parm("debug_images").set(True)
            hou.setPwd(self.root)
            ctx = HaContext.HaContext(external_hashes=map(lambda x: str(x).rjust(4,'X'), xrange(1,90)))
            graph = ctx.get_graph()
            json_files = graph.render(json_output_directory=tmppath, copy_scene_file=True)
            json_files.sort()

            self.assertEqual(len(json_files), 17, 'incorrect count files')

            json_expected_files = [ 'testRenderPressed.hip_XX13_alembic_alembic.json'
                                    ,'testRenderPressed.hip_XX15_grid_debug.json'
                                    ,'testRenderPressed.hip_XX15_grid_ifd.json'
                                    ,'testRenderPressed.hip_XX15_grid_mantra.json'
                                    ,'testRenderPressed.hip_XX15_grid_mp4.json'
                                    ,'testRenderPressed.hip_XX15_grid_reports.json'
                                    ,'testRenderPressed.hip_XX21_comp_comp.json'
                                    ,'testRenderPressed.hip_XX21_comp_debug.json'
                                    ,'testRenderPressed.hip_XX21_comp_mp4.json'
                                    ,'testRenderPressed.hip_XX21_comp_reports.json'
                                    ,'testRenderPressed.hip_XXX2_teapot_geometry.json'
                                    ,'testRenderPressed.hip_XXX4_box_geometry.json'
                                    ,'testRenderPressed.hip_XXX6_box_teapot_debug.json'
                                    ,'testRenderPressed.hip_XXX6_box_teapot_ifd.json'
                                    ,'testRenderPressed.hip_XXX6_box_teapot_mantra.json'
                                    ,'testRenderPressed.hip_XXX6_box_teapot_mp4.json'
                                    ,'testRenderPressed.hip_XXX6_box_teapot_reports.json' ]
        
            json_expected_files.sort()
            self._test_files(json_expected_files, json_files)
        
        return True



#if __name__ == '__main__':
def run():
    for test in [TestRenderPressed]: # TestMantraRenderFromIfd, TestRenderPressed, TestMantraRenderWithTilesRenderPressed
         case = unittest.TestLoader().loadTestsFromTestCase(test)
         unittest.TextTestRunner(verbosity=3).run(case)
run()