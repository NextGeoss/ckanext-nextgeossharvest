
from owslib.wfs import WebFeatureService
from owslib.fes import *
from owslib.etree import etree
import json

# OUTPUT_FORMAT is the sorted list of prefencial outputs formats from WFS
OUTPUT_FORMAT = ["application/json"]

class WFS():
    def __init__(self, url, version):
        self.wfs = WebFeatureService(url=url, version=version)

    def set_collection(self, collection):
        collection_exist = self._check_collection(collection)
        if collection_exist:
            self.collection = collection
        return collection_exist

    def _check_collection(self, collection):
        feature_types = list(self.wfs.contents)
        collection_exist = True if collection in feature_types else False
        return collection_exist

    def get_schema(self):
        if hasattr(self, 'collection'):
            return self.wfs.get_schema(self.collection)
        else:
            return None

    def make_request(self, max_dataset=100, sort_by=None, start_index=0, constraint=None):
        output_format = self._get_outputformat()
        result = self.wfs.getfeature(typename=self.collection,
                                     filter=constraint,
                                     maxfeatures=max_dataset,
                                     sortby=sort_by,
                                     startindex=start_index,
                                     outputFormat=output_format)
        result = result.read()
        if 'json' in output_format:
            return json.loads(result)
        else:
            return result

    def get_request(self, max_dataset=None, sort_by=None, start_index=0, constraint=None):
        output_format = self._get_outputformat()
        result = self.wfs.getGETGetFeatureRequest(typename=self.collection,
                                                  filter=constraint,
                                                  maxfeatures=max_dataset,
                                                  sortby=sort_by,
                                                  startindex=start_index,
                                                  outputFormat=output_format)
        
        return result

    def _get_outputformat(self):
        getfeature_param = self.wfs.getOperationByName('GetFeature').parameters
        allowed_output_format = getfeature_param["outputFormat"]["values"]

        for output_format in OUTPUT_FORMAT:
            if output_format in allowed_output_format:
                return output_format
        return None
    
    def set_filter_equal_to(self, propertyname, value):
        constraint = PropertyIsLike(propertyname=propertyname, literal=value)
        filterxml = etree.tostring(constraint.toXML()).decode("utf-8")
        return filterxml
