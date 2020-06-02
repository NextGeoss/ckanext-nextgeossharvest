VITOCGS_IGNORE_LIST = [
        "uuid",
        "StartTime",
        "StopTime",
        "description",
        "filename",
        "Collection",
        "product_download",
        "metadata_download"
]

VITOCGS_IGNORE_DICT = {
    "CGS_S1_SLC_L1": VITOCGS_IGNORE_LIST,
    "CGS_S1_GRD_L1": VITOCGS_IGNORE_LIST
}