
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
                "account_filters": [
                    {"account": "10023994", "filter_id": "10147416"}, #last_7_days
                    # {"account": "10023994", "filter_id": "10147415"}, #last_9_months
                    # {"account": "10023994", "filter_id": "10147269"}, #last_12_months
                ]
             },
            {
                "report_id": "10078516",
                "filter_id": "10141930",
                "name": "payment_trend",
                "account_filters": [
                    {"account": "10023994", "filter_id": "10147417"}, #last_7_days
                    # {"account": "10023994", "filter_id": "10147412"},#last_9_months
                    # {"account": "10023994", "filter_id": "10147267"},#last_12_months
                ]
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
            # {"id": "10034812", "name": "SAN DIEGO WELLNESS"}, # Moved to reveloop instance
            {"id": "10034187", "name": "THE TRINITY WELLNESS GROUP"},
            {"id": "10032876", "name": "AMITY PALM BEACH"},
            # {"id": "10035136", "name": "LIAH WELLNESS CENTER LLC"}, # Moved to reveloop instance
            # {"id": "10035138", "name": "LIAH WELLNESS SERVICES PLLC"}, # Deactivated
            # {"id": "10035139", "name": "THE BRIDGES OF HOUSTON LLC"},# Moved to reveloop instance
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

    # 'ca': {
    #     'api_base_url': 'https://webapi.collaboratemd.com/v1',
    #     'username': 'caapi',
    #     'password': '47e8kzWm',
    #     'accounts': [
    #         {"id": "10034987", "name": "516 RECOVERY LLC"},
    #         {"id": "10027737", "name": "ABLE TO CHANGE RECOVERY"},
    #         {"id": "10030162", "name": "APEX RECOVERY"},
    #         {"id": "10029860", "name": "ARISE HILLSIDE"},
    #         {"id": "10031658", "name": "BETTER DAYS MENTAL HEALTH"},
    #         {"id": "10033794", "name": "CALIFORNIA HEALING CENTERS"},
    #         {"id": "10031431", "name": "CALIFORNIA WELLNESS"},
    #         {"id": "10030238", "name": "CASA SERENA"},
    #         {"id": "10034004", "name": "CHANNEL ISLANDS REHAB"},
    #         {"id": "10025762", "name": "CNV DETOX INC."},
    #         {"id": "10033842", "name": "COMPASSIONATE RECOVERY ASSOCIATES"},
    #         {"id": "10027387", "name": "HARMONY GROVE RECOVERY"},
    #         {"id": "10034051", "name": "HEARTLAND WELLNESS"},
    #         {"id": "10032113", "name": "HIGHER PURPOSE FAMILY COUNSELING"},
    #         {"id": "10029742", "name": "HIGHER PURPOSE RECOVERY"},
    #         {"id": "10032400", "name": "HOPE RESTORED"},
    #         {"id": "10029965", "name": "INWARD HEALTHCARE"},
    #         {"id": "10030886", "name": "IRIS HEALING CENTER"},
    #         {"id": "10030849", "name": "IRIS HEALING RETREAT"},
    #         {"id": "10034265", "name": "KIMBERLY TAYLOR FAMILY THERAPIST"},
    #         {"id": "10023510", "name": "LA JOLLA RECOVERY - 702971"},
    #         {"id": "10024513", "name": "MALLARD LAKE RANCH DETOX CENTER LLC"},
    #         {"id": "10035042", "name": "MEDICAL ASSURE MANAGEMENT"},
    #         {"id": "10033618", "name": "MIDWEST RECOVERY CENTERS"},
    #         {"id": "10031169", "name": "NEWPORT MENTAL HEALTH CENTER"},
    #         {"id": "10035570", "name": "NMWR CHICAGO LLC"},
    #         {"id": "10029596", "name": "NO MATTER WHAT DETOX"},
    #         {"id": "10027535", "name": "NO MATTER WHAT RECOVERY"},
    #         {"id": "10035477", "name": "NORTHRIDGE RECOVERY"},
    #         {"id": "10018984", "name": "OCEAN HILLS RECOVERY - 508134"},
    #         {"id": "10025063", "name": "PACIFIC SANDS RECOVERY CENTER"},
    #         {"id": "10025605", "name": "PATH TO RECOVERY"},
    #         {"id": "10026246", "name": "RANCH CREEK RECOVERY"},
    #         {"id": "10032261", "name": "RECREATION HEALING CENTER"},
    #         {"id": "10027928", "name": "SEQUOIA DETOX CENTERS"},
    #         {"id": "10035479", "name": "SERENITY POINT THERAPY"},
    #         {"id": "10021919", "name": "SKYWARD TREATMENT SOLUTIONS LLC - 657259"},
    #         {"id": "10034342", "name": "SOBER NATION"},
    #         {"id": "10028423", "name": "SOCAL BEACH REHAB"},
    #         {"id": "10029211", "name": "SURF CITY RECOVERY"},
    #         {"id": "10022118", "name": "TAYLOR ADDICTION AND WELLNESS, LLC"},
    #         {"id": "10028812", "name": "TAYLOR RECOVERY"},
    #         {"id": "10021916", "name": "TAYLOR RECOVERY CENTER LLC - 657255"},
    #         {"id": "10031902", "name": "TEMECULA RECOVERY CENTER"},
    #         {"id": "10035945", "name": "THE CROSSING RECOVERY"},
    #         {"id": "10022291", "name": "VILLA TC"},
    #         {"id": "10029719", "name": "WEST LA RECOVERY"},
    #     ],
    #     'report_configs': [
    #         {"report_id": "10090107", "filter_id": "10146579", "name": "ar_aging"},
    #         {"report_id": "10090108", "filter_id": "10146580", "name": "benefits_call_campaign"},
    #         {"report_id": "10090109", "filter_id": "10146581", "name": "charges_on_hold"},
    #         {"report_id": "10090129", "filter_id": "10146582", "name": "claim_stage_breakdown"},
    #         {"report_id": "10090132", "filter_id": "10146583", "name": "clearing_house_rejections"},
    #         {"report_id": "10090135", "filter_id": "10146584", "name": "client_billing_reports"},
    #         {"report_id": "10090138", "filter_id": "10146585", "name": "denial_trends"},
    #         {"report_id": "10090141", "filter_id": "10146586", "name": "deposits_for_invoicing"},
    #         {"report_id": "10090150", "filter_id": "10146587", "name": "gross_billing"},
    #         {"report_id": "10090151", "filter_id": "10146588", "name": "model_data"},
    #         {"report_id": "10090155", "filter_id": "10146589", "name": "payment_trend"},
    #         {"report_id": "10090158", "filter_id": "10146590", "name": "pdr3_calculator"},
    #         {"report_id": "10090160", "filter_id": "10146591", "name": "quadrant_performance"},
    #         {"report_id": "10090161", "filter_id": "10146592", "name": "rcm_productivity"},
    #         {"report_id": "10090162", "filter_id": "10146593", "name": "rejected_at_payer"},
    #         {"report_id": "10090164", "filter_id": "10146594", "name": "rev_rec_charges"},
    #         {"report_id": "10090165", "filter_id": "10146595", "name": "rev_rec_payments"},
    #         {"report_id": "10090168", "filter_id": "10146596", "name": "revamped_weekly_ar_worklists"},
    #         {"report_id": "10090169", "filter_id": "10146597", "name": "user_time_spread"},
    #         {"report_id": "10090175", "filter_id": "10146598", "name": "write_off_trend"},
    #     ]
    # },
    #
    # 'reveloop': {
    #     'api_base_url': 'https://webapi.collaboratemd.com/v1',
    #     'username': 'reveloopapi',
    #     'password': 'HAL?V>PY}^pjq+D@X!FU',
    #     'accounts': [
    #         {"id": "10035139", "name": "THE BRIDGES OF HOUSTON LLC"},
    #         {"id": "10034812", "name": "SAN DIEGO WELLNESS"},
    #         {"id": "10035136", "name": "LIAH WELLNESS CENTER LLC"},
    #         {"id": "10034281", "name": "BILLING SERVICE ACCOUNT"},
    #         {"id": "10034288", "name": "AVIV WELLNESS CENTER"},
    #     ],
    #     'report_configs': [
    #         {"report_id": "10090319", "filter_id": "10146864", "name": "ar_aging"},
    #         {"report_id": "10090320", "filter_id": "10146865", "name": "benefits_call_campaign"},
    #         {"report_id": "10090324", "filter_id": "10146867", "name": "charges_on_hold"},
    #         {"report_id": "10090325", "filter_id": "10146868", "name": "claim_stage_breakdown"},
    #         {"report_id": "10090326", "filter_id": "10147080", "name": "clearing_house_rejections"},
    #         {"report_id": "10090331", "filter_id": "10146869", "name": "client_billing_reports"},
    #         {"report_id": "10090333", "filter_id": "10146870", "name": "denial_trends"},
    #         {"report_id": "10090339", "filter_id": "10146871", "name": "deposits_for_invoicing"},
    #         {"report_id": "10090341", "filter_id": "10146872", "name": "gross_billing"},
    #         {"report_id": "10090352", "filter_id": "10146873", "name": "model_data"},
    #         {"report_id": "10090354", "filter_id": "10146874", "name": "payment_trend"},
    #         {"report_id": "10090356", "filter_id": "10146875", "name": "pdr3_calculator"},
    #         {"report_id": "10090359", "filter_id": "10146876", "name": "quadrant_performance"},
    #         {"report_id": "10090360", "filter_id": "10146877", "name": "rcm_productivity"},
    #         {"report_id": "10090361", "filter_id": "10146878", "name": "rejected_at_payer"},
    #         {"report_id": "10090363", "filter_id": "10146879", "name": "rev_rec_charges"},
    #         {"report_id": "10090364", "filter_id": "10146880", "name": "rev_rec_payments"},
    #         {"report_id": "10090368", "filter_id": "10146881", "name": "revamped_weekly_ar_worklists"},
    #         {"report_id": "10090370", "filter_id": "10146882", "name": "user_time_spread"},
    #         {"report_id": "10090371", "filter_id": "10146883", "name": "write_off_trend"},
    #     ]
    # },
}