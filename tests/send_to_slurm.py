from hafarm import HaGraph
from hafarm import Batch
from hafarm import SlurmRender
from hafarm import GraphvizRender




item1 = Batch.BatchBase("test1",'/hafarm/test1')
item2.parms['scene_file'] = '/PROD/dev/sandbox/user/snazarenko/render/sungrid/ifd/test1'
item1.parms['command'] << { "command": "mkdir -p" }


item2 = Batch.BatchBase("test2",'/hafarm/test2')
item2.parms['scene_file'] = '/PROD/dev/sandbox/user/snazarenko/render/sungrid/ifd/test1/test2'
item2.parms['command'] << { "command": "touch" }
item2.add( item1 )




graph = HaGraph.HaGraph()

graph.add_node(item1)
graph.add_node(item2)

#graph.set_render(GraphvizRender.GraphvizRender)
graph.set_render(SlurmRender.SlurmRender)
graph.render()







