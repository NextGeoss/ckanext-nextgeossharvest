SCENT_IMAGE_IGNORE_LIST = [
        "source_id",
        "az",
        "alt",
        "elevation",
        "source_type",
        "status_desc",
        "accuracy"
]

SCENT_VIDEO_IGNORE_LIST = [
        "fps",
        "user_id",
        "status",
        "is_valid",
        "conf",
        "water_velocity"
]

SCENT_TEMP_MOIST_IGNORE_LIST = [
        "unit",
        "sensor_id",
        "value",
        "accuracy"
]

SCENT_IGNORE_DICT = {
    "SCENT_DANUBE_IMAGE": SCENT_IMAGE_IGNORE_LIST,
    "SCENT_DANUBE_VIDEO": SCENT_VIDEO_IGNORE_LIST,
    "SCENT_DANUBE_MOISTURE": SCENT_TEMP_MOIST_IGNORE_LIST,
    "SCENT_DANUBE_TEMPERATURE": SCENT_TEMP_MOIST_IGNORE_LIST,
    "SCENT_KIFISOS_IMAGE": SCENT_IMAGE_IGNORE_LIST,
    "SCENT_KIFISOS_VIDEO": SCENT_VIDEO_IGNORE_LIST,
    "SCENT_KIFISOS_MOISTURE": SCENT_TEMP_MOIST_IGNORE_LIST,
    "SCENT_KIFISOS_TEMPERATURE": SCENT_TEMP_MOIST_IGNORE_LIST
}
