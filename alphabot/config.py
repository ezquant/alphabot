import io
import yaml


config = {}

def load_config(filename):    
    with io.open(filename) as f:
        config.update(yaml.safe_load(f.read()))
    return config
