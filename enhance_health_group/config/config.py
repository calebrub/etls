# Python configuration module for enhance_health_group
#
# This file replaces the old INI-based `config/config.ini`.
# - Define INSTANCES as a dict mapping instance_key -> configuration dict
# - Each instance dict can include a "report_configs" list which is
#   a list of {"report_id": "<id>", "name": "<name>"} mappings.
#
# Keep secrets out of VCS in production. Use environment variables to override
# values via the existing ConfigLoader._get_env_override mechanism.

POSTGRES = {
    'host': 'revlooppgserver.postgres.database.azure.com',
    'user': 'REVETLCUSPRODUSER',
    'password': 'uO63mP5df9KvLhVZZHdkr3cG',
    'database': 'REVETLCUSPRODDB',
    'port': '5432',
    'schema': 'dw_data',
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
            '10026559',
            '10023851',

        ],
        'report_configs': [
            {"report_id": "10078378", "filter_id": "10141925", "name": "ar_aging"},
            {"report_id": "10078486", "filter_id": "10141929", "name": "gross_billing"},
            {"report_id": "10078375", "filter_id": "10141926", "name": "charges_on_hold"},
            {"report_id": "10078446", "filter_id": "10141927", "name": "claim_stage_breakdown"},
            {"report_id": "10078463", "filter_id": "10141928", "name": "denial_trends"},
            {"report_id": "10078516", "filter_id": "10141930", "name": "payment_trend"},
            {"report_id": "10066805", "filter_id": "10141935", "name": "rcm_productivity"},
            {"report_id": "10078520", "filter_id": "10141934", "name": "user_time_spread"},
            {"report_id": "10078521", "filter_id": "10141933", "name": "write_off_trend"},
            {"report_id": "10078522", "filter_id": "10141931", "name": "pdr3_calculator"},
            {"report_id": "10078523", "filter_id": "10141932", "name": "rev_rec_charges"},
            {"report_id": "10078524", "filter_id": "10141937", "name": "rev_rec_payments"}
        ],
    },

    'vantage': {
        'api_base_url': 'https://webapi.collaboratemd.com/v1',
        'username': 'vantagercmapi',
        'password': '_9N}TiFPE(l7xk3nH`OZ',
        'accounts': [
            '10031998',
            '10032271',
            '10031999',
            '10032272',
            '10034661',
            '10034812',
            '10034187',
            '10032876',
        ],
        # This instance uses a smaller, different report set
        'report_configs': [
            {"report_id": "10062054", "filter_id": "10137065", "name": "ar_aging"},
            {"report_id": "10062055", "filter_id": "10137067", "name": "charges_on_hold"},
            {"report_id": "10062056", "filter_id": "10137069", "name": "claim_stage_breakdown"},
            {"report_id": "10062057", "filter_id": "10137072", "name": "denial_trends"},
            {"report_id": "10062059", "filter_id": "10137074", "name": "gross_billing"},
            {"report_id": "10062060", "filter_id": "10137076", "name": "payment_trend"},
            {"report_id": "10062061", "filter_id": "10137077", "name": "quadrant_performance"},
            {"report_id": "10062064", "filter_id": "10137071", "name": "rcm_productivity"},
            {"report_id": "10062065", "filter_id": "10137078", "name": "user_time_spread"},
            {"report_id": "10062066", "filter_id": "10137079", "name": "write_off_trend"},
    ],
    },
}