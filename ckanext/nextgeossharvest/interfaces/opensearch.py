import json
import xmltodict
import requests
import datetime
from requests.auth import HTTPBasicAuth

class OPENSEARCH():

    def __init__(self, config, COLLECTION):
        self.current_url = config.get('base_query_url')
        self.max_dataset = config.get('max_dataset', 100)
        self.page_start_keyword = config.get('page_start_keyword')
        self.start_index = self.get_minimum_pagination_value()
        self.page_size_keyword = config.get('page_size_keyword')
        
        self._collection_id = config.get('collection')
        self._collection = COLLECTION[self._collection_id]

        self.timeout = config.get('timeout', 10)
        self.username = config.get('username', None)
        self.password = config.get('password', None)
        self.collection_keyword = config.get('collection_keyword', None)
        self.start_date=config.get('start_date','2015-01-01T00:00:00')
        if self.collection_keyword:
            self.collection_search = self._collection["collection_search"]
        
        self.build_url()
    
    def get_pagination_mechanism(self):
        return self.page_start_keyword

    def get_minimum_pagination_value(self):
        return 1

    @staticmethod
    def validate_config(config, COLLECTION):
        config_obj = json.loads(config)

        if 'base_query_url' not in config_obj:
            raise ValueError('The parameter base_query_url is required')

        if 'page_size_keyword' not in config_obj:
            raise ValueError('The parameter page_size_keyword is required')

        if 'page_start_keyword' not in config_obj:
            raise ValueError('The parameter page_start_keyword is required')
        
        if type(config_obj.get('collection_keyword', 'search_keyword')) != unicode:
            raise ValueError('collection_keyword must be a string')

        if 'collection' in config_obj:
            collection = config_obj['collection']
            if collection not in COLLECTION:
                err_msg = '"collection" must be one of the entries of {}'
                raise ValueError(err_msg.format(list(COLLECTION.keys())))
        else:
            raise ValueError('"collection" is required')

        if type(config_obj.get('max_dataset', 100)) != int:
            raise ValueError('max_dataset must be an integer')
        
        if type(config_obj.get('update_all', False)) != bool:
            raise ValueError('update_all must be true or false')
        
        if type(config_obj.get('timeout', 10)) != int:
            raise ValueError('timeout must be an integer')

    def build_url(self):
        if '?' in self.current_url:
            base_url, query = self.current_url.split('?')
        else:
            base_url = self.current_url
            query = ""

        if self.collection_keyword and self.collection_keyword not in query:
            query += '&{}={}'.format(self.collection_keyword, self.collection_search)
        if self.page_start_keyword not in query:
            query += '&{}={}'.format(self.page_start_keyword, str(self.start_index))
        if self.page_size_keyword not in query:
            query += '&{}={}'.format(self.page_size_keyword, str(self.max_dataset))
    
        query_components = query.split('&')
        for i, component in enumerate(query_components):
            if component.startswith(self.page_size_keyword):
                query_components[i] = '{}={}'.format(self.page_size_keyword, str(self.max_dataset))
            elif component.startswith(self.page_start_keyword):
                query_components[i] = '{}={}'.format(self.page_start_keyword, str(self.start_index))
            elif self.collection_keyword and component.startswith(self.collection_keyword):
                query_components[i] = '{}={}'.format(self.collection_keyword, self.collection_search)

        query = '&'.join(query_components)
        self.current_url = '{}?{}'.format(base_url, query)
        
    def build_url_date(self):
        if '?' in self.current_url:
            base_url, query = self.current_url.split('?')
        else:
            base_url = self.current_url
            query = ""

        if self.collection_keyword and self.collection_keyword not in query:
            query += '&{}={}'.format(self.collection_keyword, self.collection_search)
        if self.page_start_keyword not in query:
            query += '&{}={}'.format(self.page_start_keyword, str(self.start_index))
        
        if self.start_date not in query:
            query += '&{}={}'.format('start', self.start_date)
        '''datetime.datetime.strftime(datetime.datetime.strptime(self.start_date,'%Y-%m-%dT%H:%M:%S')+datetime.timedelta(days=self.start_index),'%Y-%m-%dT%H:%M:%S')'''
        
        if self.page_size_keyword not in query:
            query += '&{}={}'.format(self.page_size_keyword, str(self.max_dataset))
    
        query_components = query.split('&')
        for i, component in enumerate(query_components):
            if component.startswith(self.page_size_keyword):
                query_components[i] = '{}={}'.format(self.page_size_keyword, str(self.max_dataset))
            elif component.startswith(self.page_start_keyword):
                query_components[i] = '{}={}'.format(self.page_start_keyword, str(self.start_index))
            elif component.startswith('start'):
                query_components[i] = '{}={}'.format('start', self.start_date)  
            
            elif self.collection_keyword and component.startswith(self.collection_keyword):
                query_components[i] = '{}={}'.format(self.collection_keyword, self.collection_search)

        query = '&'.join(query_components)
        self.current_url = '{}?{}'.format(base_url, query)
    
    def update_index(self, index=None):
        # possible hook for non integer pagination mechanisms
        min_index = self.get_minimum_pagination_value()
        self.start_index = int(index) if index else min_index

    def increment_index(self):
        # possible hook for non integer pagination mechanisms
        self.start_index += 1

    def get_index(self):
        return self.start_index

    def get_results(self):
        if self.username and self.password:
            req = requests.get(self.current_url,
                            auth=HTTPBasicAuth(self.username, self.password),
                            verify=False, timeout=self.timeout)
        else:
            req = requests.get(self.current_url,
                            verify=False, timeout=self.timeout)

        if req.status_code != 200:
            return None
        else:
            content_type = req.headers['content-type']
            if 'json' in content_type:
                results = json.loads(req.text)
            elif 'xml' in content_type:
                results = xmltodict.parse(req.text)
            else:
                return None
            return results

    def get_entries_path(self):
        return self._collection["dataset_tag"]["path"]

    def get_identifier_path(self):
        mandatory_fields = self.get_mandatory_fields()
        return mandatory_fields["identifier"]["path"]

    def get_mandatory_fields(self):
        return self._collection["mandatory_fields"]

    def get_resource_fields(self):
        return self._collection["resources"]

    def get_extras_fields(self):
        return self._collection["extras"]

    def get_collection_info(self):
        collection_info = {
            "collection_id": self._collection_id,
            "collection_name": self._collection["collection_name"],
            "collection_description": self._collection["collection_description"]
        }
        return collection_info
