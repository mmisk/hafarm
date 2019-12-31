import os
import sys
import re 
import json
import pymel.core as pm
from Hamak import hamak
from hafarm import JsonRigObject



_, IP, PORT, hamak_set_maya_name, scenefilepath, scratch_ma_filepath = sys.argv


def get_line():
    file = open(scenefilepath)
    if (file.read(1).encode("hex") == '2f') == False:
        raise StopIteration
    file.seek(0)
    while True:
        line = file.readline()
        if not line:
            file.close()
            break
        yield line

gen = get_line()

search_set_name = []

with open(scratch_ma_filepath,'w') as fout:
    for line in gen:
        ret =  re.findall(r"file\s-r", line)
        if ret:
            reference_file = re.findall(r"(\/.*?\.[\w:]+)", line)
            newfilename = ''

            if reference_file != []:
                oldfilename = newfilename = reference_file[0]
                find = line
            else:
                fout.write(line)
                line = gen.next()
                reference_file = re.findall(r"(\/.*?\.[\w:]+)", line)
                if reference_file != []:
                    oldfilename = newfilename = reference_file[0]
                    find = line
                else:
                    fout.write(line)
                    find = gen.next()
                    reference_file = re.findall(r"(\/.*?\.[\w:]+)", find)
                    oldfilename = newfilename = reference_file[0]


            basefilepath, _ = os.path.split(oldfilename)
            jsonfilename = basefilepath + os.sep + 'rig.json'

            if not os.path.exists(jsonfilename):
                print "[  HA DEBUG  ] jsonfilename", oldfilename
                jsrig = JsonRigObject.JsonRigObject.create(oldfilename)
                newfilename = jsrig.path(hamak_set_maya_name)
                print "[ HA WARNING ] Json file not found %s" % jsonfilename
                if not os.path.exists(newfilename):
                    print "[ HA WARNING ] File file not found %s" % newfilename
                    newfilename = oldfilename
            else:
                with open(jsonfilename) as json_file:
                    main_description = json.load(json_file)

                export_sets = []

                latest = main_description['latest']

                for ext in latest:
                    description_file = latest[ext]

                    if not os.path.exists(description_file):
                        print "[ HA WARNING ] Json file not found %s. Skipped..." % description_file
                        continue

                    with open(description_file) as json_file:
                        description = json.load(json_file)

                    for export_set in description['export_sets']:
                        if export_set['short_name'] == hamak_set_maya_name:
                            search_set_name += [ export_set['long_name'] ]
                            newfilename = description['path']

            print "[ HA MESSAGE ] %s replace to %s " % (oldfilename, newfilename)
            fout.write(find.replace(oldfilename, newfilename))
        else:
            fout.write(line)

if os.path.exists(scratch_ma_filepath):
    os.chmod(scratch_ma_filepath, 0o0777)

# scratch_filepath, _ = os.path.split(scratch_ma_filepath)

# search_set_name = list(set(search_set_name))

# pm.openFile(scratch_ma_filepath, f=True)
# print "[ HA MESSAGE ] Open scene %s" % (scratch_ma_filepath)

# # _hamak = hamak.Hamak()
# # assets = _hamak.create_assets()
# # object_sets = _hamak.create_object_sets(assets)
 
# ret = []
# for item in object_sets:
#     name = item.get_set_name()
#     if name == None:
#         continue
#     ret += [name]
#     if name.endswith(":%s" % search_set_name[0]):
#         print "[ HA MESSAGE ] Found set name %s" % (name)
#         with open(scratch_ref_filepath,"w") as f:
#             f.write( "%s;%s" % (name,item.create_file_path()))
#             print "[ HA MESSAGE ] Changes save to %s " % (scratch_ref_filepath)
#         sys.exit(0)


# print '[  HA ERROR  ] Set not found ":%s" through %s' % ( search_set_name, ret)

# sys.exit(1)




