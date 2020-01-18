import os

class HaConstant(object):
    """ This class gives possibility to edit item by chunks
    """
    def __init__(self, default):
        self._parms = None
        self._default = default


    def set_parms(self, val):
        self._parms = val


    def __lshift__(self, str_obj):
        self._default = str_obj


    def __str__(self):

        def expand_list(a):
            if isinstance(a, list):
                return ' '.join( [str(x) for x in a ])
            return a

        parms = dict([(m,expand_list(n)) for m,n in self._parms.iteritems()])
        return os.path.expandvars( self._default.format(**parms) )
