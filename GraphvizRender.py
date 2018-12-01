# 2018.11.09 13:58:48 CET
import tempfile
from graphviz import Digraph



class GraphvizRender(object):
    """Paint graph with graphviz"""

    def __init__(self, hagraphitems_lst, **kwargs):
        super(GraphvizRender, self).__init__()
        self.hagraphitems_lst = hagraphitems_lst

    def render(self):
        dot = Digraph(comment='The Round Table')
        dot.node_attr.update(fillcolor='darkolivegreen3', style='rounded,filled', shape='rectangle')
        i = 0
        for k, n in self.hagraphitems_lst.iteritems():
            kw = {}
            dot.node(str(k), ('%s\n%s\n%s\n%s' % (n.parms['job_name'],
             n.tags,
             n.name,
             n.path)), **kw)
            i += 1

        for k, n in self.hagraphitems_lst.iteritems():
            dependencies = n.dependencies
            if len(dependencies) > 0:
                edges = map(lambda x: (str(x), str(k)), dependencies)
                for n, m in edges:
                    dot.edge(n, m)

        dot.format = 'png'
        dot.render('graph.gv', tempfile.gettempdir(), True, cleanup=True)
