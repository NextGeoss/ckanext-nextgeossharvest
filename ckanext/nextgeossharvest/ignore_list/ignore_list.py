from sentinel import SENTINEL_IGNORE_DICT
from probav import PROBAV_IGNORE_DICT
from gome2 import GOME2_IGNORE_DICT
from modis import MODIS_IGNORE_DICT
from foodsecurity import FOODSECURITY_IGNORE_DICT
from deimosimg import DEIMOSIMG_IGNORE_DICT
from plan4all import PLAN4ALL_IGNORE_DICT
from cmems import CMEMS_IGNORE_DICT

IGNORE_LIST = {}

IGNORE_LIST.update(SENTINEL_IGNORE_DICT)
IGNORE_LIST.update(PROBAV_IGNORE_DICT)
IGNORE_LIST.update(GOME2_IGNORE_DICT)
IGNORE_LIST.update(MODIS_IGNORE_DICT)
IGNORE_LIST.update(FOODSECURITY_IGNORE_DICT)
IGNORE_LIST.update(DEIMOSIMG_IGNORE_DICT)
IGNORE_LIST.update(PLAN4ALL_IGNORE_DICT)
IGNORE_LIST.update(CMEMS_IGNORE_DICT)