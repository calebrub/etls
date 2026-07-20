import os
import importlib.util

class ConfigLoader:
    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, 'config', 'config.py')
        
        spec = importlib.util.spec_from_file_location('config_module', config_path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)

    def get_postgres_config(self):
        return getattr(self.module, 'POSTGRES')

    def get_source_db_config(self):
        return getattr(self.module, 'SOURCE_DB')
