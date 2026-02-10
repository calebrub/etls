import ast
import os
import importlib.util
from configparser import RawConfigParser
from typing import Dict, List, Tuple


class ConfigLoader:
    """
    Handles loading and parsing configuration for single or multiple Collaboratmed instances.
    Supports both legacy single-instance and new multi-instance configurations.

    This loader now accepts either an INI file path (default) or a Python module file path (e.g. 'config/config.py').
    """

    def __init__(self, config_path: str = 'config/config.ini'):
        self.config_path = config_path
        self._py_config = None

        # If the config path ends with .py load it as a python module
        if config_path.endswith('.py') and os.path.exists(config_path):
            spec = importlib.util.spec_from_file_location('config_module', config_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore
            self._py_config = module
            self.config = RawConfigParser()  # keep for compatibility with INI-based methods
        else:
            self.config = RawConfigParser()
            self.config.read(config_path)

    def _get_env_override(self, section: str, key: str, fallback=None):
        """Check for environment variable override: SECTION_KEY format"""
        env_key = f"{section}_{key}".upper()
        env_value = os.getenv(env_key)
        return env_value if env_value is not None else fallback

    def _is_multi_instance_config(self) -> bool:
        """Check if config uses multi-instance format (has [INSTANCES] section)"""
        # For python config modules treat INSTANCES dict as multi-instance
        if self._py_config is not None:
            return hasattr(self._py_config, 'INSTANCES')

        return self.config.has_section('INSTANCES')

    def get_instances(self) -> Dict[str, Dict]:
        """
        Returns a dictionary of instances with their configuration.

        Multi-instance format (INI)
        [INSTANCES]
        instances = [instance_key1, instance_key2, ...]

        [INSTANCE:instance_key1]
        api_base_url = ...
        username = ...
        password = ...
        accounts = [...]

        Python config module format:
        INSTANCES = {
            'instance_key1': {
                'api_base_url': '...',
                'username': '...'
                'password': '...'
                'accounts': [...]
                'report_configs': [ {'report_id': '...', 'name': '...'}, ... ]
            }
        }

        Legacy single-instance format (INI):
        [API]
        report_api_base_url = ...
        username = ...
        password = ...

        [CUSTOMERS]
        accounts = [...]

        Returns: {
            'default': {
                'api_base_url': '...',
                'username': '...',
                'password': '...',
                'accounts': ['...'],
                'instance_key': 'default'
            },
            'instance_key1': {...},
            ...
        }
        """
        instances = {}

        if self._py_config is not None and hasattr(self._py_config, 'INSTANCES'):
            # Python module based multi-instance config
            raw_instances = getattr(self._py_config, 'INSTANCES') or {}

            for key, raw in raw_instances.items():
                # allow env overrides using SECTION_KEY style: INSTANCE_<KEY>_FIELD
                section_prefix = f'INSTANCE_{key}'.upper()

                api_base = self._get_env_override(section_prefix, 'api_base_url', raw.get('api_base_url'))
                username = self._get_env_override(section_prefix, 'username', raw.get('username'))
                password = self._get_env_override(section_prefix, 'password', raw.get('password'))
                accounts = raw.get('accounts', [])

                # ensure accounts is a list
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
            # Multi-instance mode (INI)
            instance_keys = ast.literal_eval(
                self.config.get('INSTANCES', 'instances')
            )

            for key in instance_keys:
                section = f'INSTANCE:{key}'
                if not self.config.has_section(section):
                    raise ValueError(
                        f"Instance '{key}' defined in [INSTANCES] section "
                        f"but section [{section}] not found"
                    )

                instances[key] = {
                    'instance_key': key,
                    'api_base_url': self._get_env_override(
                        section.replace(':', '_'),
                        'api_base_url',
                        self.config.get(section, 'api_base_url')
                    ),
                    'username': self._get_env_override(
                        section.replace(':', '_'),
                        'username',
                        self.config.get(section, 'username')
                    ),
                    'password': self._get_env_override(
                        section.replace(':', '_'),
                        'password',
                        self.config.get(section, 'password')
                    ),
                    'accounts': ast.literal_eval(
                        self.config.get(section, 'accounts')
                    ),
                    # No per-instance reports in INI mode; fallback to global REPORTS
                    'report_configs': []
                }
        else:
            # Legacy single-instance mode
            instances['default'] = {
                'instance_key': 'default',
                'api_base_url': self._get_env_override(
                    'API',
                    'report_api_base_url',
                    self.config.get('API', 'report_api_base_url')
                ),
                'username': self._get_env_override(
                    'API',
                    'username',
                    self.config.get('API', 'username')
                ),
                'password': self._get_env_override(
                    'API',
                    'password',
                    self.config.get('API', 'password')
                ),
                'accounts': ast.literal_eval(
                    self.config.get('CUSTOMERS', 'accounts')
                ),
                'report_configs': []
            }

        return instances

    def get_instance(self, instance_key: str) -> Dict:
        """Get configuration for a specific instance"""
        instances = self.get_instances()
        if instance_key not in instances:
            raise ValueError(
                f"Instance '{instance_key}' not found. "
                f"Available instances: {list(instances.keys())}"
            )
        return instances[instance_key]

    def get_postgres_config(self) -> Dict:
        """Get PostgreSQL configuration (shared across all instances)"""
        # If python config module provides POSTGRES, prefer it
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

        return {
            'host': self._get_env_override(
                'POSTGRES',
                'host',
                self.config.get('POSTGRES', 'host')
            ),
            'user': self._get_env_override(
                'POSTGRES',
                'user',
                self.config.get('POSTGRES', 'user')
            ),
            'password': self._get_env_override(
                'POSTGRES',
                'password',
                self.config.get('POSTGRES', 'password', fallback='')
            ),
            'database': self._get_env_override(
                'POSTGRES',
                'database',
                self.config.get('POSTGRES', 'database')
            ),
            'port': self._get_env_override(
                'POSTGRES',
                'port',
                self.config.get('POSTGRES', 'port', fallback='5432')
            ),
            'schema': self._get_env_override(
                'POSTGRES',
                'schema',
                self.config.get('POSTGRES', 'schema', fallback='public')
            ),
        }

    def get_report_configs(self, instance_key: str = None) -> List[Dict]:
        """Get report configurations.

        If instance_key is provided and the configuration is python-module-based, return
        that instance's `report_configs`. For INI-based configs this method falls back
        to the legacy REPORTS section (global across instances).
        """
        # Python module per-instance reports
        if self._py_config is not None:
            if instance_key:
                instances = self.get_instances()
                if instance_key not in instances:
                    raise ValueError(f"Instance '{instance_key}' not found")
                return instances[instance_key].get('report_configs', []) or []

            # No instance key provided: try GLOBAL_REPORTS in python config
            if hasattr(self._py_config, 'GLOBAL_REPORTS'):
                return getattr(self._py_config, 'GLOBAL_REPORTS') or []

            return []

        # INI-based legacy behavior: return global REPORTS if present
        if self.config.has_section('REPORTS'):
            report_ids = ast.literal_eval(
                self.config.get('REPORTS', 'identifiers')
            )
            report_names = self.config.options('REPORT_NAMES')

            report_configs = []
            for report_id in report_ids:
                report_id_str = str(report_id)
                if report_id_str in report_names:
                    report_configs.append({
                        'report_id': report_id_str,
                        'name': self.config.get('REPORT_NAMES', report_id_str)
                    })

            return report_configs

        return []

    def list_instances(self) -> List[str]:
        """Get list of all available instance keys"""
        return list(self.get_instances().keys())

    def validate_instances(self) -> Tuple[bool, List[str]]:
        """
        Validate that all instances have required fields.
        Returns (is_valid, error_messages)
        """
        errors = []
        instances = self.get_instances()

        for key, config in instances.items():
            if not config.get('api_base_url'):
                errors.append(f"Instance '{key}': missing api_base_url")
            if not config.get('username'):
                errors.append(f"Instance '{key}': missing username")
            if not config.get('password'):
                errors.append(f"Instance '{key}': missing password")
            if not config.get('accounts') or not isinstance(config['accounts'], list):
                errors.append(f"Instance '{key}': missing or invalid accounts list")

            # Validate per-instance report_configs if present
            reports = config.get('report_configs', [])
            if reports and not isinstance(reports, list):
                errors.append(f"Instance '{key}': report_configs must be a list")
            elif isinstance(reports, list):
                seen_ids = set()
                for r in reports:
                    if not isinstance(r, dict) or 'report_id' not in r:
                        errors.append(f"Instance '{key}': each report must be a dict with 'report_id'")
                        continue
                    if r['report_id'] in seen_ids:
                        errors.append(f"Instance '{key}': duplicate report_id {r['report_id']} in report_configs")
                    seen_ids.add(r['report_id'])

        return len(errors) == 0, errors

