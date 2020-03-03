COLLECTION = {
    ###################################################
    ##############          FS1          ##############
    ###################################################

    ###################################################
    ############## GRF Processors Config ##############
    ###################################################
    ##############        Level 1        ##############
    "FS1_GRF_L1A_CAL": {
        "collection_id": "FSSCAT_GRF_L1A_CAL",
        "collection_name": "FSSCAT GNSS Reflectometer Calibrated L1A",
        "collection_description": "GNSS Calibration (inc. Clock bias)"
    },
    "FS1_GRF_L1A_SCI": {
        "collection_id": "FSSCAT_GRF_L1A_SCI",
        "collection_name": "FSSCAT GNSS Reflectometer Science L1A",
        "collection_description": "GNSS Reflectometer calibrated DDM Peak"
    },
    "FS1_GRF_L1A_TLM": {
        "collection_id": "FSSCAT_GRF_L1A_TLM",
        "collection_name": "FSSCAT GNSS Reflectometer Telemetry L1A",
        "collection_description": "	GNSS Telemetry (containing positions of transmitted signals)"
    },
    "FS1_GRF_L1B_CAL": {
        "collection_id": "FSSCAT_GRF_L1B_CAL",
        "collection_name": "FSSCAT GRF_L1B_CAL",
        "collection_description": "TBD"
    },
    "FS1_GRF_L1B_SCI": {
        "collection_id": "FSSCAT_GRF_L1B_SCI",
        "collection_name": "FSSCAT GNSS Reflectometer Science L1B",
        "collection_description": "GNSS Reflectometer L1A and geolocation SP"
    },
    ##############        Level 2        ##############
    "FS1_GRF_L2__ICM": {
        "collection_id": "FSSCAT_GRF_L2__ICM",
        "collection_name": "FSSCAT GNSS Reflectometer Ice Map L2",
        "collection_description": "Ice Map product"
    },
    ##############        Level 3        ##############
    "FS1_GRF_L3__ICM": {
        "collection_id": "FSSCAT_GRF_L3__ICM",
        "collection_name": "FSSCAT GNSS Reflectometer Ice Map L3",
        "collection_description": "Ice Presence Map L3 gridded"
    },
    ###################################################
    ############## MWR Processors Config ##############
    ###################################################
    ##############        Level 1        ##############
    "FS1_MWR_L1A_CAL": {
        "collection_id": "FSSCAT_MWR_L1A_CAL",
        "collection_name": "FSSCAT Microwave L-band Calibration L1A",
        "collection_description": "Calibration: gain etc. when measuring ref loads (i.e. not in EO) "
    },
    "FS1_MWR_L1A_SCI": {
        "collection_id": "FSSCAT_MWR_L1A_SCI",
        "collection_name": "FSSCAT Microwave L-band Science L1A",
        "collection_description": "TA Antenna Temperature"
    },
    "FS1_MWR_L1A_TLM": {
        "collection_id": "FSSCAT_MWR_L1A_TLM",
        "collection_name": "FSSCAT Microwave L-band Telemetry L1A",
        "collection_description": "TBD"
    },
    "FS1_MWR_L1B_CAL": {
        "collection_id": "FSSCAT_MWR_L1B_CAL",
        "collection_name": "FSSCAT Microwave L-band Calibration L1B",
        "collection_description": "Full Fov Brightness Temperature"
    },
    "FS1_MWR_L1B_SCI": {
        "collection_id": "FSSCAT_MWR_L1B_SCI",
        "collection_name": "FSSCAT Microwave L-band Science L1B",
        "collection_description": "Geolocated Brightness Temperature"
    },
    "FS1_MWR_L1C_SCI": {
        "collection_id": "FSSCAT_MWR_L1C_SCI",
        "collection_name": "FSSCAT Microwave L-band Science L1C",
        "collection_description": "Gridded Brightness Temperature map"
    },

    ##############        Level 4        ##############
    ##############          TBD          ##############

    ###################################################
    ############## NAV Processors Config ##############
    ###################################################
    ##############        Level 1        ##############

    "FS1_NAV_L1__GEO": {
        "collection_id": "FSSCAT_NAV_L1__GEO",
        "collection_name": "FSSCAT Navigation Telemetry",
        "collection_description": "Satellite navigation data - Orbit and Attitude from Telemetry"
    },


    ###################################################
    ##############          FS2          ##############
    ###################################################

    ###################################################
    ############## HPS Processors Config ##############
    ###################################################
    ##############        Level 1        ##############

    "FS2_HPS_L1B_SCI": {
        "collection_id": "FSSCAT_HPS_L1B_SCI",
        "collection_name": "FSSCAT Hyperscout Science L1",
        "collection_description": "Hyperscout Radiances Calibrated & Geolocated"
    },
    ###################################################
    ############## NAV Processors Config ##############
    ###################################################
    ##############        Level 1        ##############

    "FS2_NAV_L1__GEO": {
        "collection_id": "FSSCAT_NAV_L1__GEO",
        "collection_name": "FSSCAT Navigation Telemetry",
        "collection_description": "Satellite navigation data - Orbit and Attitude from Telemetry"
    },

    ###################################################
    ##############          FSS          ##############
    ###################################################

    ###################################################
    ############## SYN Processors Config ##############
    ###################################################
    ##############        Level 1        ##############


    "FSS_SYN_L4__SM_": {
        "collection_id": "FSSCAT_SYN_L4__SM_",
        "collection_name": "FSSCAT Soil Moisture",
        "collection_description": "Soil Moisture from MWR+HPS Synergy"
    }
}