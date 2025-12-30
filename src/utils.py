import os
import shutil

def cleanup(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
