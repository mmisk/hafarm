import json, sys, os, re, glob, logging, subprocess
import pymel.core as pm

logging.basicConfig(level=logging.DEBUG)


_, json_filepath = sys.argv

camera_descriptor = "renderCam"
renderTemplate_descriptor = "RDR-renderTemplate"
assembly_descriptor = "RDR-assembly"
shading_descriptor = "RDR-shading"
asset_types = ["prop","char","set"]


def find_asset_type(asset_name,asset_types):
  logging.info("FIND ASSET TYPES:")

  project_path = os.path.join(os.environ.get('JOB_PROJECT_DIR'),os.environ.get('JOB_CURRENT'))
  for asset_type in asset_types:
    if asset_name in os.listdir(os.path.join(project_path,asset_type)):
      return asset_type
  logging.warning("for project: {}".format(project_path))
  logging.warning("can't find asset: {} in {}".format(asset_name,asset_types))


def find_latest_version(file_descriptor,folder_path):
  logging.info("FIND LATEST VERSION:")

  v = 0
  latest_version_path = None
  re_pattern = "("+file_descriptor+")"+"_(v\\d+)"


  for single_file in glob.glob(os.path.join(folder_path,"*"+file_descriptor+"*")):
    logging.info("looking for file descriptor in :" + single_file)
    result = re.search(re_pattern,single_file)
    version_string = result.group(2)
    version_string = re.sub("[^\d\.]", "", version_string)
    current_version = int(version_string)

    if (current_version > v):
      v = current_version
      latest_version_path = single_file

  logging.info("returning: "+str(latest_version_path))
  if latest_version_path is None:
    logging.warning("cant find any file with a version with \"{}\" in filename in directory {} \n".format(file_descriptor,folder_path))

  return latest_version_path

def getAnimationVersion(json_filepath):
  with open(json_filepath) as json_file:
    json_data = json.load(json_file)

  animation_file_version = json_data["version"]
  logging.info("aniamtion file version: " + str(animation_file_version))
  return animation_file_version
  

def import_assets(json_file_path,shading_descriptor):

  logging.info("importing assets from "+ json_file_path)

  with open(json_file_path) as json_file:
    json_data = json.load(json_file)

  #iterate over assets
  for asset in json_data["scene"]:

    asset_name = asset.split("_")[0]

    #do not need this
    #asset_instance = asset.split("_")[1]

    logging.info("importing asset: "+ asset)

    #find asset type
    asset_type = find_asset_type(asset_name,asset_types)
    if asset_type is None:
      continue
      
    #find shading scene for asset
    asset_shading_directory = os.path.join(os.environ.get('JOB_PROJECT_DIR'),os.environ.get('JOB_CURRENT'),asset_type,asset_name,"software","maya","scenes","public")
    asset_shading_filepath = find_latest_version(shading_descriptor,asset_shading_directory)
    if asset_shading_filepath is None:
      logging.warning("can't find shading scene for "+asset)
      continue
    asset_shading_basename = os.path.basename(asset_shading_filepath)

    logging.info("asset_shading_basename: " + asset_shading_basename)

    # making a copy of a shading scene
    temp_shading_scene = os.path.join(os.environ['HA_SCRATCH'],asset_shading_basename)

    copy_command = "cp " + asset_shading_filepath + " " + temp_shading_scene

    logging.info("copy command: "+copy_command)
    subprocess.call(copy_command, shell=True)

    logging.info("copying " + asset_shading_filepath +" to " + temp_shading_scene)

    #change alembic cache paths in shading scene
    with open(temp_shading_scene) as f:
          temp_shading_scene_content = f.read()

    #iterating over assets in json 
    for cache in json_data["scene"][asset]:
      cache_name = cache
      cache_path = os.path.join(os.environ["JOB"],json_data["scene"][asset][cache_name])

      logging.info("cache_name: " + cache_name)
      logging.info("cache_path: " + cache_path)

      pattern = "(/.*/"+cache_name+"/.*.abc)"

      logging.info("pattern: "+ pattern)

      found_strings = re.findall(pattern,temp_shading_scene_content)

      #replace chace paths 
      temp_shading_scene_content = re.sub(pattern,cache_path,temp_shading_scene_content)

      for i in found_strings:
        logging.info("replaceing " + i + " with " + cache_path)

    #saving changed file
    with open(temp_shading_scene, 'w') as f:
      f.write(temp_shading_scene_content)
        
    #Maya import temp shading scenes
    logging.info("loading temp shading scene: "+temp_shading_scene)
    pm.importFile(temp_shading_scene,force=True,preserveReferences=True, namespace=asset)

    #delete temp shading scene
    try:
          os.remove(temp_shading_scene)
          logging.info("deleted file: " + temp_shading_scene)
    except OSError as e:  ## if failed, report it back to the user ##
          logging.err("Error: %s - %s." % (e.filename, e.strerror))



def import_camera(json_filepath, camera_descriptor):
  camera_directory = os.getenv('JOB') + "/" + "camera" + "/abc"
  camera_filepath = find_latest_version(camera_descriptor,camera_directory)
  pm.importFile(camera_filepath)

  camera_name = None

  for n in pm.ls(type='camera'):
    camera_name = n.name()
    if os.environ['JOB_ASSET_NAME'] in camera_name:
      break
  else:
    raise Exception("Camera not found")

  return camera_name, camera_filepath



def open_renderTemplate(json_filepath,renderTemplate_descriptor,assembly_descriptor,animationFile_version):
  renderTemplate_directory = os.path.join(os.getenv('JOB'),"software","maya","scenes","public")
  renderTemplate_filepath = find_latest_version(renderTemplate_descriptor,renderTemplate_directory)
  logging.info(renderTemplate_filepath)

  pm.openFile(renderTemplate_filepath,force=True)

  assembly_root_path = os.path.join(os.getenv('JOB'),"software","maya","scenes","public")
  assembly_filepath = os.path.join(assembly_root_path,os.getenv('JOB_ASSET_NAME')+"_"+assembly_descriptor+"_"+"v"+str(animationFile_version)+".ma")

  logging.info("assembly_filepath:"+assembly_filepath)
  print "[ HA MESSAGE ] Save file: ", assembly_filepath
  pm.renameFile(assembly_filepath)
  
  
  return assembly_filepath



def set_frame_range(json_filepath):

  with open(json_filepath) as json_file:
    json_data = json.load(json_file)

  startFrame = json_data["frames"]["start"]
  endFrame = json_data["frames"]["end"]

  logging.info("setting frame range to: {} {}".format(startFrame,endFrame))
  pm.playbackOptions(min=startFrame, max=endFrame)
  render_globals = pm.PyNode('defaultRenderGlobals')
  render_globals.startFrame.set(int(startFrame))
  render_globals.endFrame.set(int(endFrame))
  pm.saveFile()

## MAKE THE SCENE ##

animation_file_version = getAnimationVersion(json_filepath)

assembly_filepath = open_renderTemplate(json_filepath,renderTemplate_descriptor,assembly_descriptor,animation_file_version)
import_assets(json_filepath,shading_descriptor)
camera_name, camera_filepath = import_camera(json_filepath,camera_descriptor) 
set_frame_range(json_filepath)


with open(json_filepath) as json_file:
  json_data = json.load(json_file)
  json_data['filename'] = assembly_filepath
  json_data['camera'] = camera_name
  json_data['camera_filepath'] = camera_filepath

with open(json_filepath, "w") as json_file:
  json.dump(json_data, json_file, indent=4)