COLLECTION = {
    ###################################################
    ############## GRF Processors Config ##############
    ###################################################
    ##############        Level 1        ##############
    ###################################################
    "GRF_L1C_SCI": {
        "collection_id": "FSSCAT_GRF_L1C_SCI",
        "collection_name": "FSSCAT GNSS Reflectometer L1C Science",
        "collection_description": "GNSS-R Geolocated Reflection Observables"
    },
    "GRF_L1B_CAL": {
        "collection_id": "FSSCAT_GRF_L1B_CAL",
        "collection_name": "FSSCAT GNSS Reflectometer L1B Calibration",
        "collection_description": "GNSS-R Geolocated Direct Delay-Doppler Maps"
    },
    "GRF_L1B_SCI": {
        "collection_id": "FSSCAT_GRF_L1B_SCI",
        "collection_name": "FSSCAT GNSS Reflectometer L1B Science",
        "collection_description": "GNSS-R Geolocated Reflection Delay-Doppler Maps"
    },
    ###################################################
    ##############        Level 2        ##############
    ###################################################
    "GRF_L2__SIE": {
        "collection_id": "FSSCAT_GRF_L2__SIE",
        "collection_name": "FSSCAT GNSS Reflectometer L2 Ice Map",
        "collection_description": "GNSS-R Detection Sea Ice Extent Map"
    },
    ###################################################
    ##############        Level 3        ##############
    ###################################################
    "GRF_L3__ICM": {
        "collection_id": "FSSCAT_GRF_L3__SIE",
        "collection_name": "FSSCAT GNSS Reflectometer L3 Ice Map",
        "collection_description": "GNSS-R Detection Sea Ice Extent Map Composite"
    },
    ###################################################
    ############## MWR Processors Config ##############
    ###################################################
    ##############        Level 1        ##############
    ###################################################
    "MWR_L1B_SCI": {
        "collection_id": "FSSCAT_MWR_L1B_SCI",
        "collection_name": "FSSCAT Microwave Radiometer L1B Science",
        "collection_description": "MWR L-Band Geolocated Instrument Brightness Temperature"
    },
    "MWR_L1C_SCI": {
        "collection_id": "FSSCAT_MWR_L1C_SCI",
        "collection_name": "FSSCAT Microwave Radiometer L1C Science",
        "collection_description": "MWR L-Band Brightness Temperature Map"
    },

    ###################################################
    ##############        Level 2        ##############
    ###################################################
    "MWR_L2A_TB_": {
        "collection_id": "FSSCAT_MWR_L2A_TB_",
        "collection_name": "FSSCAT Microwave Radiometer L2A Brightness Temperature",
        "collection_description": "MWR L-Band Ice/Land Brightness Temperature Map"
    },
    "MWR_L2B_SIT": {
        "collection_id": "FSSCAT_MWR_L2B_SIT",
        "collection_name": "FSSCAT Microwave Radiometer L2B Sea Ice Thickness",
        "collection_description": "MWR L-Band Sea Ice Thickness Map"
    },
    "MWR_L2B_SM_": {
        "collection_id": "FSSCAT_MWR_L2B_SM_",
        "collection_name": "FSSCAT Microwave Radiometer L2B Soil Moisture",
        "collection_description": "MWR L-Band Soil Moisture Map"
    },

    ###################################################
    ##############        Level 3        ##############
    ###################################################
    "MWR_L3__TB_": {
        "collection_id": "FSSCAT_MWR_L3__TB_",
        "collection_name": "FSSCAT Microwave Radiometer L3 Ice/Land Brightness Temperature",
        "collection_description": "L-Band Ice/Land Brightness Temperature Map Composite"
    },
    "MWR_L3__SIT": {
        "collection_id": "FSSCAT_MWR_L3__SIT",
        "collection_name": "FSSCAT Microwave Radiometer L3 Sea Ice Thickness",
        "collection_description": "MWR L-Band Sea Ice Thickness Map Composite"
    },
    "MWR_L3__SM_": {
        "collection_id": "FSSCAT_MWR_L3__SM_",
        "collection_name": "FSSCAT Microwave Radiometer L3 Soil Moisture",
        "collection_description": "MWR L-Band Soil Moisture Map Composite"
    },
    ###################################################
    ##############        Level 4        ##############
    ###################################################
    "MWR_L4__SM_": {
        "collection_id": "FSSCAT_MWR_L4__SM_",
        "collection_name": "FSSCAT Microwave Radiometer L4 Downscaled Soil Moisture",
        "collection_description": "Soil Moisture from MWR+NDVI Downscaling"
    },

    ###################################################
    ############## HPS Processors Config ##############
    ###################################################
    ##############        Level 1        ##############
    ###################################################
    "HPS_L1C_SCI": {
        "collection_id": "FSSCAT_HPS_L1C_SCI",
        "collection_name": "FSSCAT Hyperscout L1C Science",
        "collection_description": "Hyperscout Calibrated & Geolocated  Radiances"
    },
    ###################################################
    #############         Level 2        ##############
    ###################################################
    "HPS_L2__RDI": {
        "collection_id": "FSSCAT_HPS_L2__RDI",
        "collection_name": "FSSCAT Hyperscout L2 Radiometric Indexes",
        "collection_description": "Hyperscout Radiometric Indexes (NDVI)"
    },

    ###################################################
    ############## SYN Processors Config ##############
    ###################################################
    ##############        Level 4        ##############
    ###################################################
    "SYN_L4__SM_": {
        "collection_id": "FSSCAT_SYN_L4__SM_",
        "collection_name": "FSSCAT Synergy L4 Downscaled Soil Moisture",
        "collection_description": "Soil Moisture from MWR+HPS Downscaling"
    }
}

