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
    ##############        Level 3        ##############
    ###################################################
    "GRF_L3__OBS": {
        "collection_id": "FSSCAT_GRF_L3__OBS",
        "collection_name": "FSSCAT GNSS Reflectometer L3 Observables",
        "collection_description": "GNSS-R Reflectivity and DDM SNR Observables"
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
    "MWR_L3__CTB": {
        "collection_id": "FSSCAT_MWR_L3__CTB",
        "collection_name": "FSSCAT Microwave Radiometer L3 Raw Brightness Temperature",
        "collection_description": "L-Band Ice/Land Brightness Temperature Map Composite from L1C"
    },
    "MWR_L3__TB_": {
        "collection_id": "FSSCAT_MWR_L3__TB_",
        "collection_name": "FSSCAT Microwave Radiometer L3 Ice/Land Brightness Temperature",
        "collection_description": "L-Band Ice/Land Brightness Temperature Map Composite from L2A"
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
    ##############        FMP SIT        ##############
    ###################################################
    "FMP_L2__SIT": {
        "collection_id": "FSSCAT_FMP_L2__SIT",
        "collection_name": "FSSCAT Neural Network L2 Sea Ice Thickness",
        "collection_description": "FMPL-2 L2 Neural Network Sea Ice Thickness Maps"
    },
    "FMP_L3__SIT": {
        "collection_id": "FSSCAT_FMP_L3__SIT",
        "collection_name": "FSSCAT Neural Network L3 Sea Ice Thickness",
        "collection_description": "FMPL-2 L3 Neural Network Sea Ice Thickness Maps"
    },
    "FMP_L4__SIT": {
        "collection_id": "FSSCAT_FMP_L4__SIT",
        "collection_name": "FSSCAT Neural Network L4 Sea Ice Thickness",
        "collection_description": "FMPL-2 L4 Neural Network Sea Ice Thickness Maps"
    },

    ###################################################
    ##############        FMP SIC        ##############
    ###################################################
    "FMP_L2__SIC": {
        "collection_id": "FSSCAT_FMP_L2__SIC",
        "collection_name": "FSSCAT Neural Network L2 Sea Ice Concentration",
        "collection_description": "FMPL-2 L2 Neural Network Sea Ice Concentration Maps"
    },
    "FMP_L3__SIC": {
        "collection_id": "FSSCAT_FMP_L3__SIC",
        "collection_name": "FSSCAT Neural Network L3 Sea Ice Concentration",
        "collection_description": "FMPL-2 L3 Neural Network Sea Ice Concentration Maps"
    },

    ###################################################
    ##############        FMP SIE        ##############
    ###################################################
    "FMP_L2__SIE": {
        "collection_id": "FSSCAT_FMP_L2__SIE",
        "collection_name": "FSSCAT Neural Network L2 Sea Ice Extent",
        "collection_description": "FMPL-2 L2 Neural Network Sea Ice Extent Maps"
    },
    "FMP_L3__SIE": {
        "collection_id": "FSSCAT_FMP_L3__SIE",
        "collection_name": "FSSCAT Neural Network L3 Sea Ice Extent",
        "collection_description": "FMPL-2 L3 Neural Network Sea Ice Extent Maps"
    },
    "FMP_L4__SIE": {
        "collection_id": "FSSCAT_FMP_L4__SIE",
        "collection_name": "FSSCAT Neural Network L4 Sea Ice Extent",
        "collection_description": "FMPL-2 L4 Neural Network Sea Ice Extent Maps"
    },

    ###################################################
    ##############        FMP SM_        ##############
    ###################################################
    "FMP_L2__SM_": {
        "collection_id": "FSSCAT_FMP_L2__SM_",
        "collection_name": "FSSCAT Neural Network L2 Soil Moisture",
        "collection_description": "FMPL-2 L2 Neural Network Soil Moisture Maps"
    },
    "FMP_L3__SM_": {
        "collection_id": "FSSCAT_FMP_L3__SM_",
        "collection_name": "FSSCAT Neural Network L3 Soil Moisture",
        "collection_description": "FMPL-2 L3 Neural Network Soil Moisture Maps"
    },
    "FMP_L4__SM_": {
        "collection_id": "FSSCAT_FMP_L4__SM_",
        "collection_name": "FSSCAT Neural Network L4 Soil Moisture",
        "collection_description": "FMPL-2 L4 Neural Network Soil Moisture Maps"
    }
}
