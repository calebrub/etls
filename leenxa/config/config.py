# Leenxa-specific configuration
# This file follows the same structure as combined/config/config.py

POSTGRES = {
    'host': 'revlooppgserver.postgres.database.azure.com',
    'user': 'REVETLCUSPRODUSER',
    'password': 'uO63mP5df9KvLhVZZHdkr3cG',
    'database': 'REVETLCUSPRODDB',
    'port': '5432',
    'schema': 'leenxa',
}

INSTANCES = {
    'enhance_health': {
        'api_base_url': 'https://webapi.collaboratemd.com/v1',
        'username': 'ehgdeiapi',
        'password': '%JdI/vt5b`Vmu8/x#F9P',
        'accounts': [
            '10028395',
            '10026936',
            '10026716',
            '10023994',
            '10026559',  # '10023851', Rebel Removed
        ],
        'account_names': {
            '10028395': 'CK FAMILY THERAPY',
            '10026936': 'EDEN BY ENHANCE',
            '10026716': 'EMBRACE TREATMENT, LLC',
            '10023994': 'ENHANCE HEALTH GROUP, LLC',
            '10026559': '10026559',
        },
        'report_configs': [
            {"report_id": "YOUR_REPORT_ID", "name": "model_data", "filter_id": "10141925"}
        ],
    },
}
