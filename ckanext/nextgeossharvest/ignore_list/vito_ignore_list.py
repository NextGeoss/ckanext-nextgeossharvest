SENTINEL_2_TOC_V2_IGNORE_LIST = ['orbit_direction', 'orbit_number', 'relative_orbit_number', 'tile_id', 'cloud_cover', 'processing_center', 'status']

CGS_S1_GRD_L1_IGNORE_LIST = ['operational_mode', 'orbit_direction', 'orbit_number', 'polarisation_channels', 'polarisation_mode', 'relative_orbit_number', 'processing_center', 'status']

CGS_S1_SLC_L1_IGNORE_LIST = ['operational_mode', 'orbit_direction', 'orbit_number', 'polarisation_channels', 'polarisation_mode', 'relative_orbit_number', 'processing_center', 'status']

CGS_S1_GRD_SIGMA0_L1_IGNORE_LIST = ['operational_mode', 'orbit_direction', 'orbit_number', 'polarisation_channels', 'polarisation_mode', 'relative_orbit_number', 'processing_center', 'status']

TERRASCOPE_SENTINEL_2_LAI_V2_IGNORE_LIST = ['orbit_direction', 'relative_orbit_number', 'tile_id', 'resolution', 'cloud_cover', 'processing_center', 'status']

TERRASCOPE_SENTINEL_2_NDVI_V2_IGNORE_LIST = ['orbit_direction', 'relative_orbit_number', 'tile_id', 'resolution', 'cloud_cover', 'processing_center', 'status']

TERRASCOPE_SENTINEL_2_FAPAR_V2_IGNORE_LIST = ['orbit_direction', 'relative_orbit_number', 'tile_id', 'resolution', 'cloud_cover', 'processing_center', 'status']

TERRASCOPE_SENTINEL_2_FCOVER_V2_IGNORE_LIST = ['orbit_direction', 'relative_orbit_number', 'tile_id', 'resolution', 'cloud_cover', 'processing_center', 'status']


VITO_IGNORE_DICT = {
    "SENTINEL_2_TOC_V2": "SENTINEL_2_TOC_V2_IGNORE_LIST",
    "CGS_S1_GRD_L1": "CGS_S1_GRD_L1_IGNORE_LIST",
    "CGS_S1_SLC_L1": "CGS_S1_SLC_L1_IGNORE_LIST",
    "CGS_S1_GRD_SIGMA0_L1": "CGS_S1_GRD_SIGMA0_L1_IGNORE_LIST",
    "TERRASCOPE_SENTINEL_2_LAI_V2": "TERRASCOPE_SENTINEL_2_LAI_V2_IGNORE_LIST",
    "TERRASCOPE_SENTINEL_2_NDVI_V2": "TERRASCOPE_SENTINEL_2_NDVI_V2_IGNORE_LIST",
    "TERRASCOPE_SENTINEL_2_FAPAR_V2": "TERRASCOPE_SENTINEL_2_FAPAR_V2_IGNORE_LIST",
    "TERRASCOPE_SENTINEL_2_FCOVER_V2": "TERRASCOPE_SENTINEL_2_FCOVER_V2_IGNORE_LIST"
}