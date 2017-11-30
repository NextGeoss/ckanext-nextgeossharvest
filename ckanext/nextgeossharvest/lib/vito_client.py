import logging

from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo, SortBy, SortProperty

log = logging.getLogger(__name__)

class BaseError(Exception):
    pass


class BaseService(object):
    def __init__(self, endpoint=None):
        if endpoint is not None:
            self._ows(endpoint)


class VitoService(BaseService):
    '''
    Preform various operations on a Vito
    '''
