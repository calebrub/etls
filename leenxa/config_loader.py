import ast
import importlib.util
import os
from configparser import RawConfigParser
from typing import Dict, List


class ConfigLoader:
    """
    Handles loading and parsing configuration for Leenxa instances.
    Supports both legacy single-instance and new multi-instance configurations.
    """

    def __init__(self, config_path: str = 'config/config.py'):
        self.config_path = config_path
        self._py_config = None

        # If the config path ends with .py load it as a python module
        if config_path.endswith('.py') and os.path.exists(config_path):
            spec = importlib.util.spec_from_file_location('config_module', config_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore
            self._py_config = module
            self.config = RawConfigParser()
        else:
            self.config = RawConfigParser()
            self.config.read(config_path)

    def _get_env_override(self, section: str, key: str, fallback=None):
        """Check for environment variable override: SECTION_KEY format"""
        env_key = f"{section}_{key}".upper()
        env_value = os.getenv(env_key)
        return env_value if env_value is not None else fallback

    def _is_multi_instance_config(self) -> bool:
        """Check if config uses multi-instance format"""
        if self._py_config is not None:
            return hasattr(self._py_config, 'INSTANCES')
        return self.config.has_section('INSTANCES')

    def get_instances(self) -> Dict[str, Dict]:
        instances = {}

        if self._py_config is not None and hasattr(self._py_config, 'INSTANCES'):
            raw_instances = getattr(self._py_config, 'INSTANCES') or {}

            for key, raw in raw_instances.items():
                section_prefix = f'INSTANCE_{key}'.upper()

                api_base = self._get_env_override(section_prefix, 'api_base_url', raw.get('api_base_url'))
                username = self._get_env_override(section_prefix, 'username', raw.get('username'))
                password = self._get_env_override(section_prefix, 'password', raw.get('password'))
                accounts = raw.get('accounts', [])

                if isinstance(accounts, str):
                    try:
                        accounts = ast.literal_eval(accounts)
                    except Exception:
                        accounts = [accounts]

                instances[key] = {
                    'instance_key': key,
                    'api_base_url': api_base,
                    'username': username,
                    'password': password,
                    'accounts': accounts,
                    'report_configs': raw.get('report_configs', [])
                }

        elif self._is_multi_instance_config():
            instance_keys = ast.literal_eval(self.config.get('INSTANCES', 'instances'))
            for key in instance_keys:
                section = f'INSTANCE:{key}'
                instances[key] = {
                    'instance_key': key,
                    'api_base_url': self._get_env_override(section.replace(':', '_'), 'api_base_url', self.config.get(section, 'api_base_url')),
                    'username': self._get_env_override(section.replace(':', '_'), 'username', self.config.get(section, 'username')),
                    'password': self._get_env_override(section.replace(':', '_'), 'password', self.config.get(section, 'password')),
                    'accounts': ast.literal_eval(self.config.get(section, 'accounts')),
                    'report_configs': []
                }
        return instances

    def get_postgres_config(self) -> Dict:
        if self._py_config is not None and hasattr(self._py_config, 'POSTGRES'):
            pg = getattr(self._py_config, 'POSTGRES')
            return {
                'host': self._get_env_override('POSTGRES', 'host', pg.get('host')),
                'user': self._get_env_override('POSTGRES', 'user', pg.get('user')),
                'password': self._get_env_override('POSTGRES', 'password', pg.get('password', '')),
                'database': self._get_env_override('POSTGRES', 'database', pg.get('database')),
                'port': self._get_env_override('POSTGRES', 'port', pg.get('port', '5432')),
                'schema': self._get_env_override('POSTGRES', 'schema', pg.get('schema', 'public')),
            }
        return {}

    def list_instances(self) -> List[str]:
        return list(self.get_instances().keys())
