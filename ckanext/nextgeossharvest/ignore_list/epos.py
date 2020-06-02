EPOS_IGNORE_LIST = [
        "StartDate",
        "Startdate",
        "product_url",
        "thumbnail",
        "size",
        "InstrumentName",
        "AcquisitionType",
        "ProductQualityStatus",
        "FamilyName",
        "InstrumentFamilyName"
]

EPOS_IGNORE_DICT = {
    "WRAPPED_INTERFEROGRAM": EPOS_IGNORE_LIST,
    "UNWRAPPED_INTERFEROGRAM": EPOS_IGNORE_LIST,
    "SPATIAL_COHERENCE": EPOS_IGNORE_LIST,
    "MAP_OF_LOS_VECTOR": EPOS_IGNORE_LIST,
    "INTERFEROGRAM_APS_GLOBAL_MODEL": EPOS_IGNORE_LIST,
    "LOS_DISPLACEMENT_TIMESERIES": EPOS_IGNORE_LIST,
    "DEM_RADAR_GEOMETRY": EPOS_IGNORE_LIST,
    "LOOKUP_TABLE_RADAR2GROUND_COORDINATES": EPOS_IGNORE_LIST
}