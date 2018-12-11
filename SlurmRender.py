import pprint
from managers.slurm import Slurm
from itertools import cycle

class SlurmRender(object):
    def __init__(self, hagraphitems_lst, **kwargs):
        super(SlurmRender, self).__init__()
        self.hagraphitems_lst = hagraphitems_lst

    def render(self, dryrun=False):
        for k, n in self.hagraphitems_lst.iteritems():
            for i in n.dependencies:
                item = self.hagraphitems_lst[i]
                n.parms['hold_jid'] += [item.parms['job_name']]

        items_idx_sended = []
        items_idx = self.hagraphitems_lst.keys()
        cycle_items_idx = cycle(items_idx)
        slurm_obj = Slurm()
        while items_idx != []:
            idx = next(cycle_items_idx)
            item = self.hagraphitems_lst[idx]
            if set(item.dependencies) <= set(items_idx_sended):
                items_idx.remove(idx)
                cycle_items_idx = cycle(items_idx)
                items_idx_sended += [idx]
                slurm_obj.render(item.parms,dryrun=dryrun)
