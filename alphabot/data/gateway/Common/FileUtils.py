from os import listdir
from os.path import isfile, join, exists
from .CommonUtils import *
from Config import Config

def get_all_files(path: str):
    if not exists(path):
        return []
    return [f for f in listdir(path) if isfile(join(path, f))]

def convert_file_path_based_on_system(path: str):
    sys = get_os_system()
    if sys == Config.OsSystem.WINDOWS:
        return path.replace("/", "\\")
    elif sys == Config.OsSystem.MAC:
        return path.replace("\\", "/")
    raise Exception("unknown system")