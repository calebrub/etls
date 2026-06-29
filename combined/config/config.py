
POSTGRES = {
    'host': 'revlooppgserver.postgres.database.azure.com',
    'user': 'REVETLCUSPRODUSER',
    'password': 'uO63mP5df9KvLhVZZHdkr3cG',
    'database': 'REVETLCUSPRODDB',
    'port': '5432',
    'schema': 'dw_combined',
}

INSTANCES = {
    'enhance_health': {
        'api_base_url': 'https://webapi.collaboratemd.com/v1',
        'username': 'ehgdeiapi',
        'password': '%JdI/vt5b`Vmu8/x#F9P',
        'accounts': [
            {"id": "10028395", "name": "CK FAMILY THERAPY"},
            {"id": "10026936", "name": "EDEN BY ENHANCE"},
            {"id": "10026716", "name": "EMBRACE TREATMENT, LLC"},
            {"id": "10023994", "name": "ENHANCE HEALTH GROUP, LLC"},
            {"id": "10026559", "name": "VIRTUAL TREATMENT CENTER LLC"},  # '10023851', Rebel Removed
        ],
        # To override filter_id per customer account, add: "account_filters": [{"account": "12345", "filter_id": "67890"}]
        'report_configs': [
            {"report_id": "10078378", "filter_id": "10141925", "name": "ar_aging"},
            {"report_id": "10078486", "filter_id": "10141929", "name": "gross_billing"},
            {"report_id": "10078375", "filter_id": "10141926", "name": "charges_on_hold"},
            {"report_id": "10078446", "filter_id": "10141927", "name": "claim_stage_breakdown"},
            {
                "report_id": "10078463",
                "filter_id": "10141928",
                "name": "denial_trends",
                "account_filters": [{"account": "10023994", "filter_id": "10147269"}] #last_12_months
             },
            {
                "report_id": "10078516",
                "filter_id": "10141930",
                "name": "payment_trend",
                "account_filters": [{"account": "10023994", "filter_id": "10147267"}] #last_12_months
             },
            {"report_id": "10066805", "filter_id": "10141935", "name": "rcm_productivity"},
            {"report_id": "10078520", "filter_id": "10141934", "name": "user_time_spread"},
            {"report_id": "10078521", "filter_id": "10141933", "name": "write_off_trend"},
            {"report_id": "10078522", "filter_id": "10144452", "name": "pdr3_calculator"},
            {"report_id": "10078523", "filter_id": "10141932", "name": "rev_rec_charges"},
            {
                "report_id": "10078524",
                "filter_id": "10141937",
                "name": "rev_rec_payments",
                "account_filters": [{"account": "10023994", "filter_id": "10147270"}] #last_12_months
            }
        ],
    },

    'vantage': {
        'api_base_url': 'https://webapi.collaboratemd.com/v1',
        'username': 'vantagercmapi',
        'password': '_9N}TiFPE(l7xk3nH`OZ',
        'accounts': [
            {"id": "10031998", "name": "1 SOLUTION WELLNESS"},
            {"id": "10032271", "name": "AMITY SAN DIEGO"},
            {"id": "10032272", "name": "NORTHRIDGE ADDICTION TREATMENT CENTERS"},
            {"id": "10034661", "name": "PASSAGE TO RECOVERY"},
            {"id": "10034812", "name": "SAN DIEGO WELLNESS"},
            {"id": "10034187", "name": "THE TRINITY WELLNESS GROUP"},
            {"id": "10032876", "name": "AMITY PALM BEACH"},
            {"id": "10035136", "name": "LIAH WELLNESS CENTER LLC"},
            # {"id": "10035138", "name": "LIAH WELLNESS SERVICES PLLC"}, # Deactivated
            {"id": "10035139", "name": "THE BRIDGES OF HOUSTON LLC"},
            # {"id": "10031999", "name": "BILLING SERVICE ACCOUNT"}, # Billing Service Account, No Data
        ],

        'report_configs': [
            {"report_id": "10062054", "filter_id": "10137065", "name": "ar_aging"},
            {"report_id": "10083396", "filter_id": "10143555", "name": "charges_on_hold"},
            {"report_id": "10062056", "filter_id": "10137069", "name": "claim_stage_breakdown"},
            {"report_id": "10062057", "filter_id": "10137072", "name": "denial_trends"},
            {"report_id": "10062059", "filter_id": "10137074", "name": "gross_billing"},
            {"report_id": "10062060", "filter_id": "10137076", "name": "payment_trend"},
            {"report_id": "10062061", "filter_id": "10137077", "name": "quadrant_performance"},
            {"report_id": "10062064", "filter_id": "10137071", "name": "rcm_productivity"},
            {"report_id": "10062065", "filter_id": "10146954", "name": "user_time_spread"},
            {"report_id": "10062066", "filter_id": "10137079", "name": "write_off_trend"},
            {"report_id": "10085814", "filter_id": "10144335", "name": "pdr3_calculator"},
            {"report_id": "10085817", "filter_id": "10144336", "name": "rev_rec_charges"},
            {"report_id": "10085815", "filter_id": "10144337", "name": "rev_rec_payments"}
    ],
    },
}