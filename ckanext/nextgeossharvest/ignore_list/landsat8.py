LANDSAT8_IGNORE_LIST = [
        "sensor",
        "satellite",
        "correction_level",
        "path",
        "row",
        "ingestion_date",
        "collection",
        "category",
        "sun_azimuth",
        "sun_elevation",
        "cloud_coverage_land"
]

LANDSAT8_IGNORE_DICT = {
    "LANDSAT_8_RT": LANDSAT8_IGNORE_LIST,
    "LANDSAT_8_T1": LANDSAT8_IGNORE_LIST,
    "LANDSAT_8_T2": LANDSAT8_IGNORE_LIST
}
