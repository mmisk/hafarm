from HaGraph import HaGraphItem
import sys

current_context = None

if 'hou' in sys.modules.keys():
    import Houdini
    from Houdini import HaContextHoudini
    current_context = HaContextHoudini()

if 'nuke' in sys.modules.keys():
    import Nuke
    from Nuke import HaContextNuke
    current_context = HaContextNuke()


from HaGraph import HaGraphItem

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
    context_class = current_context

    def __init__(self, external_hashes=[]):
        HaGraphItem._external_hashes = (lambda: [(yield x) for x in external_hashes ])()

    @plugin_property_
    def get_graph(self, **kwargs):
        pass

