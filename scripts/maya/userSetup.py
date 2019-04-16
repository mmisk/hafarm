import pymel.core as pm



def render_on_farm(*args):
    from hafarm import Maya
    reload(Maya)
    from hafarm.Maya import MayaFarmGUI

    farm = MayaFarmGUI()
    farm.create()
    farm.show()



def _addMenu():
    if pm.menu("Ha_mainMenu", exists = True) == True:
        pm.menuItem(divider=True, dividerLabel="Hafarm", p="Ha_mainMenu")
        pm.menuItem(label='Render on farm', p="Ha_mainMenu", c=render_on_farm)



def HaMenu():
    for n in pm.lsUI(l=True, menus=True):
        #if pm.objectTypeUI(n) == "menuBarLayout":
        if 'Ha_mainMenu' in n:
            _addMenu()
            break
    
pm.evalDeferred(HaMenu)