
POSTGRES = {
    'host': 'revlooppgserver.postgres.database.azure.com',
    'user': 'REVETLCUSPRODUSER',
    'password': 'uO63mP5df9KvLhVZZHdkr3cG',
    'database': 'REVETLCUSPRODDB',
    'port': '5432',
    'schema': 'staging',
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
            {"report_id": "10078378", "filter_id": "10147336", "name": "ar_aging"},  # Active: all_tableau | Options: this_week: 10147337, this_month: 10147338, yesterday: 10147339, last_30_days: 10147688
            {"report_id": "10078486", "filter_id": "10141929", "name": "gross_billing"},  # Active: tableau | Options: this_week: 10147348, this_month: 10147349, yesterday: 10147350, last_30_days: 10147699
            {"report_id": "10078375", "filter_id": "10141926", "name": "charges_on_hold"},  # Active: tableau | Options: this_week: 10147200, this_month: 10147201, yesterday: 10147202, last_30_days: 10147690
            {"report_id": "10078446", "filter_id": "10147340", "name": "claim_stage_breakdown"},  # Active: all_tableau | Options: this_week: 10147341, this_month: 10147342, yesterday: 10147343, last_30_days: 10147691
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
            {"report_id": "10066805", "filter_id": "10147221", "name": "rcm_productivity"},  # Active: tableau | Options: this_week: 10147222, this_month: 10147223, yesterday: 10147224, last_30_days: 10147704
            {"report_id": "10078520", "filter_id": "10147246", "name": "user_time_spread"},  # Active: all_tableau | Options: this_week: 10147247, this_month: 10147248, yesterday: 10147249, last_30_days: 10147709
            {"report_id": "10078521", "filter_id": "10147252", "name": "write_off_trend"},  # Active: all_tableau | Options: this_week: 10147253, this_month: 10147254, yesterday: 10147255, last_30_days: 10147710
            {"report_id": "10078522", "filter_id": "10141931", "name": "pdr3_calculator"},  # Active: tableau | Options: this_week: 10147359, this_month: 10147360, yesterday: 10147361, last_30_days: 10147701
            {"report_id": "10078523", "filter_id": "10141932", "name": "rev_rec_charges"},  # Active: tableau | Options: this_week: 10147229, this_month: 10147230, yesterday: 10147231, last_30_days: 10147706
            {
                "report_id": "10078524",
                "filter_id": "10141937",
                "name": "rev_rec_payments",
                "account_filters": [{"account": "10023994", "filter_id": "10147270"}] #last_12_months
            },
            {"report_id": "10091708", "filter_id": "10147196", "name": "benefits_call_campaign"},  # Active: all_tableau | Options: this_week: 10147197, this_month: 10147198, yesterday: 10147199, last_30_days: 10147689
            {"report_id": "10091711", "filter_id": "10147203", "name": "clearing_house_rejections"},  # Active: all_tableau | Options: this_week: 10147204, this_month: 10147205, yesterday: 10147206, last_30_days: 10147692
            {"report_id": "10091714", "filter_id": "10147207", "name": "client_billing_reports"},  # Active: all_tableau | Options: this_week: 10147208, this_month: 10147209, yesterday: 10147210, last_30_days: 10147693
            {"report_id": "10091718", "filter_id": "10147695", "name": "deposits_for_invoicing"},  # Active: all_tableau | Options: this_week: 10147696, this_month: 10147697, yesterday: 10147698, last_30_days: 10147694
            {"report_id": "10091946", "filter_id": "10147351", "name": "model_data"},  # Active: all_tableau | Options: this_week: 10147372, this_month: 10147373, yesterday: 10147374, last_30_days: 10147700
            {"report_id": "10091724", "filter_id": "10147217", "name": "quadrant_performance"},  # Active: tableau | Options: this_week: 10147218, this_month: 10147219, yesterday: 10147220, last_30_days: 10147702
            {"report_id": "10091725", "filter_id": "10147225", "name": "rejected_at_payer"},  # Active: all_tableau | Options: this_week: 10147226, this_month: 10147227, yesterday: 10147228, last_30_days: 10147705
            {"report_id": "10091726", "filter_id": "10147232", "name": "revamped_weekly_ar_worklists"}  # Active: all_tableau | Options: this_week: 10147233, this_month: 10147234, yesterday: 10147235, last_30_days: 10147708
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
            {"report_id": "10062054", "filter_id": "10146962", "name": "ar_aging"},  # Active: all_tableau | Options: this_week: 10146963, this_month: 10146964, yesterday: 10146965, last_30_days: 10147629
            {"report_id": "10083396", "filter_id": "10143555", "name": "charges_on_hold"},  # Active: tableau | Options: this_week: 10146969, this_month: 10147631, yesterday: 10147632, last_30_days: 10147633
            {"report_id": "10062056", "filter_id": "10146939", "name": "claim_stage_breakdown"},  # Active: all_tableau | Options: this_week: 10146940, this_month: 10146941, yesterday: 10146942, last_30_days: 10147634
            {"report_id": "10062057", "filter_id": "10137072", "name": "denial_trends"},  # Active: tableau | Options: this_week: 10146978, this_month: 10146979, yesterday: 10146980, last_30_days: 10147637
            {"report_id": "10062059", "filter_id": "10137074", "name": "gross_billing"},  # Active: tableau | Options: this_week: 10146946, this_month: 10146947, yesterday: 10146948
            {"report_id": "10062060", "filter_id": "10137076", "name": "payment_trend"},  # Active: tableau | Options: this_week: 10146999, this_month: 10147000, yesterday: 10147001, last_30_days: 10147651
            {"report_id": "10062061", "filter_id": "10137077", "name": "quadrant_performance"},  # Active: tableau | Options: this_week: 10147005, this_month: 10147006, yesterday: 10147007, last_30_days: 10147649
            {"report_id": "10062064", "filter_id": "10137071", "name": "rcm_productivity"},  # Active: tableau | Options: this_week: 10147646, this_month: 10147647, yesterday: 10147648, last_30_days: 10147645
            {"report_id": "10062065", "filter_id": "10146954", "name": "user_time_spread"},  # Active: all_tableau | Options: this_week: 10146955, this_month: 10146956, yesterday: 10146957, last_30_days: 10147640
            {"report_id": "10062066", "filter_id": "10146958", "name": "write_off_trend"},  # Active: all_tableau | Options: this_week: 10146959, this_month: 10146960, yesterday: 10146961, last_30_days: 10147639
            {"report_id": "10085814", "filter_id": "10144335", "name": "pdr3_calculator"},  # Active: tableau | Options: this_week: 10147002, this_month: 10147003, yesterday: 10147004, LAST_30_DAYS: 10147650
            {"report_id": "10085817", "filter_id": "10144336", "name": "rev_rec_charges"},  # Active: tableau | Options: this_week: 10147008, this_month: 10147009, yesterday: 10147010, last_30_days: 10147643
            {"report_id": "10085815", "filter_id": "10144337", "name": "rev_rec_payments"},  # Active: tableau | Options: this_week: 10147011, this_month: 10147012, yesterday: 10147013, last_30_days: 10147642
            {"report_id": "10062052", "filter_id": "10146938", "name": "benefits_call_campaign"},  # Active: all_tableau | Options: this_week: 10146966, this_month: 10146967, yesterday: 10146968, last_30_days: 10147630
            {"report_id": "10062067", "filter_id": "10146943", "name": "clearing_house_rejections"},  # Active: all_tableau | Options: this_week: 10146994, this_month: 10146995, yesterday: 10146996, last_30_days: 10147635
            {"report_id": "10074011", "filter_id": "10146944", "name": "client_billing_reports", "skip_load": True},  # Active: all_tableau | Options: this_week: 10147031, this_month: 10147032, yesterday: 10147033, last_30_days: 10147638
            {"report_id": "10075405", "filter_id": "10146945", "name": "deposits_for_invoicing"},  # Active: all_tableau | Options: this_week: 10146981, this_month: 10146982, yesterday: 10146983, last_30_days: 10147653
            {"report_id": "10075127", "filter_id": "10145145", "name": "model_data"},  # Active: all_filter | Options: this_week: 10147368, this_month: 10147369, yesterday: 10147370, last_30_days: 10147652
            {"report_id": "10062068", "filter_id": "10146949", "name": "rejected_at_payer"},  # Active: all_tableau | Options: this_week: 10146950, this_month: 10146951, yesterday: 10146952, last_30_days: 10147644
            {"report_id": "10062063", "filter_id": "10146953", "name": "revamped_weekly_ar_worklists"}  # Active: all_tableau | Options: this_week: 10147015, this_month: 10147016, yesterday: 10147017, last_30_days: 10147641
        ],
    },

    'ca': {
        'api_base_url': 'https://webapi.collaboratemd.com/v1',
        'username': 'caapi',
        'password': '47e8kzWm',
        'accounts': [
            {"id": "10034987", "name": "516 RECOVERY LLC"},
            {"id": "10027737", "name": "ABLE TO CHANGE RECOVERY"},
            {"id": "10030162", "name": "APEX RECOVERY"},
            {"id": "10029860", "name": "ARISE HILLSIDE"},
            {"id": "10031658", "name": "BETTER DAYS MENTAL HEALTH"},
            {"id": "10033794", "name": "CALIFORNIA HEALING CENTERS"},
            {"id": "10031431", "name": "CALIFORNIA WELLNESS"},
            {"id": "10030238", "name": "CASA SERENA"},
            {"id": "10034004", "name": "CHANNEL ISLANDS REHAB"},
            {"id": "10025762", "name": "CNV DETOX INC."},
            {"id": "10033842", "name": "COMPASSIONATE RECOVERY ASSOCIATES"},
            {"id": "10027387", "name": "HARMONY GROVE RECOVERY"},
            {"id": "10034051", "name": "HEARTLAND WELLNESS"},
            {"id": "10032113", "name": "HIGHER PURPOSE FAMILY COUNSELING"},
            {"id": "10029742", "name": "HIGHER PURPOSE RECOVERY"},
            {"id": "10032400", "name": "HOPE RESTORED"},
            {"id": "10029965", "name": "INWARD HEALTHCARE"},
            {"id": "10030886", "name": "IRIS HEALING CENTER"},
            {"id": "10030849", "name": "IRIS HEALING RETREAT"},
            {"id": "10034265", "name": "KIMBERLY TAYLOR FAMILY THERAPIST"},
            {"id": "10023510", "name": "LA JOLLA RECOVERY - 702971"},
            {"id": "10024513", "name": "MALLARD LAKE RANCH DETOX CENTER LLC"},
            {"id": "10035042", "name": "MEDICAL ASSURE MANAGEMENT"},
            {"id": "10033618", "name": "MIDWEST RECOVERY CENTERS"},
            {"id": "10031169", "name": "NEWPORT MENTAL HEALTH CENTER"},
            {"id": "10035570", "name": "NMWR CHICAGO LLC"},
            {"id": "10029596", "name": "NO MATTER WHAT DETOX"},
            {"id": "10027535", "name": "NO MATTER WHAT RECOVERY"},
            {"id": "10035477", "name": "NORTHRIDGE RECOVERY"},
            {"id": "10018984", "name": "OCEAN HILLS RECOVERY - 508134"},
            {"id": "10025063", "name": "PACIFIC SANDS RECOVERY CENTER"},
            {"id": "10025605", "name": "PATH TO RECOVERY"},
            {"id": "10026246", "name": "RANCH CREEK RECOVERY"},
            {"id": "10032261", "name": "RECREATION HEALING CENTER"},
            {"id": "10027928", "name": "SEQUOIA DETOX CENTERS"},
            {"id": "10035479", "name": "SERENITY POINT THERAPY"},
            {"id": "10021919", "name": "SKYWARD TREATMENT SOLUTIONS LLC - 657259"},
            {"id": "10034342", "name": "SOBER NATION"},
            {"id": "10028423", "name": "SOCAL BEACH REHAB"},
            {"id": "10029211", "name": "SURF CITY RECOVERY"},
            {"id": "10022118", "name": "TAYLOR ADDICTION AND WELLNESS, LLC"},
            {"id": "10028812", "name": "TAYLOR RECOVERY"},
            {"id": "10021916", "name": "TAYLOR RECOVERY CENTER LLC - 657255"},
            {"id": "10031902", "name": "TEMECULA RECOVERY CENTER"},
            {"id": "10035945", "name": "THE CROSSING RECOVERY"},
            {"id": "10022291", "name": "VILLA TC"},
            {"id": "10029719", "name": "WEST LA RECOVERY"},
        ],
        'report_configs': [
            {"report_id": "10090107", "filter_id": "10146579", "name": "ar_aging"},  # Active: all_tableau | Options: this_week: 10147021, this_month: 10147022, yesterday: 10147023, last_30_days: 10147713
            {"report_id": "10090108", "filter_id": "10146580", "name": "benefits_call_campaign"},  # Active: all_tableau | Options: this_week: 10146900, this_month: 10146901, yesterday: 10146902, last_30_days: 10147714
            {"report_id": "10090109", "filter_id": "10146581", "name": "charges_on_hold"},  # Active: all_tableau | Options: this_week: 10147025, this_month: 10147026, yesterday: 10147027, last_30_days: 10147715
            {"report_id": "10090129", "filter_id": "10146582", "name": "claim_stage_breakdown"},  # Active: all_tableau | Options: this_week: 10146903, this_month: 10146904, yesterday: 10146905, last_30_days: 10147716
            {"report_id": "10090132", "filter_id": "10146583", "name": "clearing_house_rejections"},  # Active: all_tableau | Options: this_week: 10147028, this_month: 10147029, yesterday: 10147030, last_30_days: 10147717
            {"report_id": "10090135", "filter_id": "10146584", "name": "client_billing_reports"},  # Active: all_tableau | Options: this_week: 10147034, this_month: 10147035, yesterday: 10147718, last_30_days: 10147719
            {"report_id": "10090138", "filter_id": "10146585", "name": "denial_trends"},  # Active: all_tableau | Options: this_week: 10147037, this_month: 10147038, yesterday: 10147039, last_30_days: 10147720
            {"report_id": "10090141", "filter_id": "10146586", "name": "deposits_for_invoicing"},  # Active: all_tableau | Options: this_week: 10147042, this_month: 10147043, yesterday: 10147044, last_30_days: 10147721
            {"report_id": "10090150", "filter_id": "10146587", "name": "gross_billing"},  # Active: all_tableau | Options: this_week: 10146906, this_month: 10146907, yesterday: 10146908, last_30_days: 10147722
            {"report_id": "10090151", "filter_id": "10146588", "name": "model_data"},  # Active: all_tableau | Options: this_week: 10147378, this_month: 10147379, yesterday: 10147380, last_30_days: 10147723
            {"report_id": "10090155", "filter_id": "10146589", "name": "payment_trend"},  # Active: all_tableau | Options: this_week: 10147051, this_month: 10147052, yesterday: 10147053, last_30_days: 10147724
            {"report_id": "10090158", "filter_id": "10146590", "name": "pdr3_calculator"},  # Active: all_tableau | Options: this_week: 10147054, this_month: 10147055, yesterday: 10147056, last_30_days: 10147725
            {"report_id": "10090160", "filter_id": "10146591", "name": "quadrant_performance"},  # Active: all_tableau | Options: this_week: 10147057, this_month: 10147058, yesterday: 10147059, last_30_days: 10147726
            {"report_id": "10090161", "filter_id": "10146592", "name": "rcm_productivity"},  # Active: all_tableau | Options: this_week: 10146909, this_month: 10146910, yesterday: 10146911, last_30_days: 10147727
            {"report_id": "10090162", "filter_id": "10146593", "name": "rejected_at_payer"},  # Active: all_tableau | Options: this_week: 10146913, this_month: 10146914, last_30_days: 10147728
            {"report_id": "10090164", "filter_id": "10146594", "name": "rev_rec_charges"},  # Active: all_tableau | Options: this_week: 10147060, this_month: 10147061, yesterday: 10147063, last_30_days: 10147729
            {"report_id": "10090165", "filter_id": "10146595", "name": "rev_rec_payments"},  # Active: all_tableau | Options: this_week: 10147064, this_month: 10147065, yesterday: 10147066, last_30_days: 10147730
            {"report_id": "10090168", "filter_id": "10146596", "name": "revamped_weekly_ar_worklists"},  # Active: all_tableau | Options: this_week: 10147067, this_month: 10147068, yesterday: 10147069, last_30_days: 10147731
            {"report_id": "10090169", "filter_id": "10146597", "name": "user_time_spread"},  # Active: all_tableau | Options: this_week: 10146916, this_month: 10146917, yesterday: 10146918, last_30_days: 10147732
            {"report_id": "10090175", "filter_id": "10146598", "name": "write_off_trend"},  # Active: all_tableau | Options: this_week: 10147070, this_month: 10147071, yesterday: 10147072, last_30_days: 10147733
        ]
    },
    'reveloop': {
        'api_base_url': 'https://webapi.collaboratemd.com/v1',
        'username': 'reveloopapi',
        'password': 'HAL?V>PY}^pjq+D@X!FU',
        'accounts': [
            {"id": "10035139", "name": "THE BRIDGES OF HOUSTON LLC"},
            {"id": "10034812", "name": "SAN DIEGO WELLNESS"},
            {"id": "10035136", "name": "LIAH WELLNESS CENTER LLC"},
            {"id": "10034281", "name": "BILLING SERVICE ACCOUNT"},
            {"id": "10034288", "name": "AVIV WELLNESS CENTER"},
        ],
        'report_configs': [
            {"report_id": "10090319", "filter_id": "10146864", "name": "ar_aging"},  # Active: all_tableau | Options: this_week: 10147074, this_month: 10147075, yesterday: 10147076
            {"report_id": "10090320", "filter_id": "10146865", "name": "benefits_call_campaign"},  # Active: all_tableau | Options: this_week: 10146919, this_month: 10146920, yesterday: 10146921
            {"report_id": "10090324", "filter_id": "10146867", "name": "charges_on_hold"},  # Active: all_tableau | Options: this_week: 10147077, this_month: 10147078, yesterday: 10147079
            {"report_id": "10090325", "filter_id": "10146868", "name": "claim_stage_breakdown"},  # Active: all_tableau | Options: this_week: 10146922, this_month: 10146923, yesterday: 10146924
            {"report_id": "10090326", "filter_id": "10147080", "name": "clearing_house_rejections"},  # Active: all_tableau | Options: this_week: 10147081, this_month: 10147082, yesterday: 10147083
            {"report_id": "10090331", "filter_id": "10146869", "name": "client_billing_reports"},  # Active: all_tableau | Options: this_week: 10147084, this_month: 10147085, yesterday: 10147086
            {"report_id": "10090333", "filter_id": "10146870", "name": "denial_trends"},  # Active: all_tableau
            {"report_id": "10090339", "filter_id": "10146871", "name": "deposits_for_invoicing"},  # Active: all_tableau | Options: this_week: 10147087, this_month: 10147088, yesterday: 10147089
            {"report_id": "10090341", "filter_id": "10146872", "name": "gross_billing"},  # Active: all_tableau | Options: this_week: 10146925, this_month: 10146926, yesterday: 10146927
            {"report_id": "10090352", "filter_id": "10146873", "name": "model_data"},  # Active: all_tableau | Options: this_week: 10147381, this_month: 10147382, yesterday: 10147383
            {"report_id": "10090354", "filter_id": "10146874", "name": "payment_trend"},  # Active: all_tableau | Options: this_week: 10147093, this_month: 10147094, yesterday: 10147095
            {"report_id": "10090356", "filter_id": "10146875", "name": "pdr3_calculator"},  # Active: all_tableau | Options: this_week: 10147096, this_month: 10147097, yesterday: 10147098
            {"report_id": "10090359", "filter_id": "10146876", "name": "quadrant_performance"},  # Active: all_tableau | Options: this_week: 10147099, this_month: 10147100, yesterday: 10147101
            {"report_id": "10090360", "filter_id": "10146877", "name": "rcm_productivity"},  # Active: all_tableau | Options: this_week: 10146928, this_month: 10146930, yesterday: 10146931
            {"report_id": "10090361", "filter_id": "10146878", "name": "rejected_at_payer"},  # Active: Filter IDs | Options: this_week: 10146932, this_month: 10146933, yesterday: 10146934
            {"report_id": "10090363", "filter_id": "10146879", "name": "rev_rec_charges"},  # Active: all_tableau | Options: this_week: 10147102, this_month: 10147103, yesterday: 10147104
            {"report_id": "10090364", "filter_id": "10146880", "name": "rev_rec_payments"},  # Active: all_tableau | Options: this_week: 10147105, this_month: 10147106, yesterday: 10147107
            {"report_id": "10090368", "filter_id": "10146881", "name": "revamped_weekly_ar_worklists"},  # Active: all_tableau | Options: this_week: 10147108, this_month: 10147109, yesterday: 10147110
            {"report_id": "10090370", "filter_id": "10146882", "name": "user_time_spread"},  # Active: all_tableau | Options: this_week: 10146935, this_month: 10146936, yesterday: 10146937
            {"report_id": "10090371", "filter_id": "10146883", "name": "write_off_trend"},  # Active: all_tableau | Options: this_week: 10147111, this_month: 10147112, yesterday: 10147113
        ]
    },
}