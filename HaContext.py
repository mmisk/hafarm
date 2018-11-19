
# TODO: There is a place for plugin to choose current context
import Houdini
# from hafarm import Houdini
reload(Houdini)
from Houdini import HaContextHoudini


def plugin_property_(func):
    def wrapper_property_(instance, *args, **kwargs):
        context_class = instance.context_class
        private_attribute_name = '_%s' % func.__name__
        if hasattr(context_class, private_attribute_name) == False:
            raise Exception('%s has no "%s". You have to implement it' % (context_class, private_attribute_name))
        func_ = context_class.__getattribute__(private_attribute_name)
        return func_(*args, **kwargs) 
    return wrapper_property_        


class HaContext(object):
    context_class = HaContextHoudini()
    def __init__(self):
        super(HaContext, self).__init__()

    @plugin_property_
    def get_graph(self, **kwargs):
        pass

def render():
    ctx = HaContext()
    graph = ctx.get_graph()
    for n in graph.graph_items:
        print n.index
