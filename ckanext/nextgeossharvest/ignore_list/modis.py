MODIS_IGNORE_LIST = [
        "uuid",
        "StartTime",
        "StopTime",
        "manifest_url",
        "download_url",
        "size",
        "Filename",
        "evi_thumbnail",
        "ndvi_thumbnail",
        "lai_thumbnail",
        "fpar_thumbnail",
        "thumbnail_1",
        "thumbnail_2",
]

MODIS_IGNORE_DICT = {
    "MODIS_AQUA_MYD13Q1": MODIS_IGNORE_LIST,
    "MODIS_AQUA_MYD13A1": MODIS_IGNORE_LIST,
    "MODIS_AQUA_MYD13A2": MODIS_IGNORE_LIST,
    "MODIS_TERRA_MOD13Q1": MODIS_IGNORE_LIST,
    "MODIS_TERRA_MOD13A1": MODIS_IGNORE_LIST,
    "MODIS_TERRA_MOD13A2": MODIS_IGNORE_LIST,
    "MOD17A3H": MODIS_IGNORE_LIST,
    "MOD17A2H": MODIS_IGNORE_LIST,
    "MODIS_AQUA_MYD15A2H": MODIS_IGNORE_LIST,
    "MODIS_TERRA_MOD15A2H": MODIS_IGNORE_LIST,
    "MODIS_TERRA_MOD14A2": MODIS_IGNORE_LIST,
    "MODIS_AQUA_MYD14A2": MODIS_IGNORE_LIST
}