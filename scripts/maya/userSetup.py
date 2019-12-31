import os
import pymel.core as pm

# FIX: rez env maya -- maya
os.environ['PATH'] = "%s:/usr/local/slurm/current/bin"%os.environ['PATH']



def render_on_farm(*args):
    from hafarm import Maya
    reload(Maya)
    from hafarm.Maya import MayaFarmGUI

    farm = MayaFarmGUI()
    farm.create_render_panel()
    farm.show()



def _addMenu(ha):
    if pm.menu(ha, exists = True) == True:
        pm.menuItem(divider=True, dividerLabel="Hafarm", p=ha)
        pm.menuItem(label='Render on farm', p=ha, c=render_on_farm)



def HaMenu():
    for n in pm.lsUI(l=True, menus=True):
        #if pm.objectTypeUI(n) == "menuBarLayout":
        # print "##############", n
        if ('Ha_mainMenu' in n) or ('HaMainMenu' in n):
            _addMenu(n)
            break

    
pm.evalDeferred(HaMenu)