FOODSECURITY_IGNORE_LIST = [
        "uuid",
        "Collection",
        "Description",
        "manifest_url",
        "thumbnail_url",
        "download_url",
        "filename"
]

FOODSECURITY_IGNORE_DICT = {
    "NEXTGEOSS_SENTINEL2_FAPAR": FOODSECURITY_IGNORE_LIST,
    "NEXTGEOSS_SENTINEL2_FCOVER": FOODSECURITY_IGNORE_LIST,
    "NEXTGEOSS_SENTINEL2_LAI": FOODSECURITY_IGNORE_LIST,
    "NEXTGEOSS_SENTINEL2_NDVI": FOODSECURITY_IGNORE_LIST
}