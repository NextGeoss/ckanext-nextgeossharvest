import json
import xmltodict
import requests
from requests.auth import HTTPBasicAuth

def get_entries(content, json_path_list):
    tag = json_path_list.pop(0)
    if len(json_path_list) == 0:
        if type(content[tag]) is not list:
            return [content[tag]]
        else:
            return content[tag]
    else:
        entries = get_entries(content[tag], json_path_list)
        return entries
        

class OPENSEARCH():
    
    @staticmethod
    def get_pagination_mechanism():
        return 'startIndex'
    
    @staticmethod
    def get_mininum_pagination_value():
        return 1

    @staticmethod
    def validate_config(config, COLLECTION):
        config_obj = json.loads(config)

        if 'base_query_url' not in config_obj:
            raise ValueError('The parameter base_query_url is required')

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

        if 'results_format' in config_obj:
            if config_obj['results_format'] not in ['json', 'xml']:
                err_msg = '"results_format" must be one of the entries of {}'
                raise ValueError(err_msg.format(['json', 'xml']))
        
        if type(config_obj.get('timeout', 10)) != int:
            raise ValueError('timeout must be an integer')

    def __init__(self, config, COLLECTION):
        self.current_url = config.get('base_query_url')
        self.max_dataset = config.get('max_dataset', 100)
        self.orderby = config.get('sortby', None)
        self.start_index = self.get_mininum_pagination_value()
        self.build_url()
        
        collection_id = config.get('collection')
        self._collection = COLLECTION[collection_id]

        self.results_format = config.get('results_format')
        self.timeout = config.get('timeout', 10)
        self.username = config.get('username', None)
        self.password = config.get('password', None)

    def build_url(self):
        base_url, query = self.current_url.split('?')
        if self.get_pagination_mechanism() not in query:
            query += '&' + self.get_pagination_mechanism() + '=' + str(self.start_index)
        if 'count=' not in query:
            query += '&count=' + str(self.max_dataset)
        query_components = query.split('&')
        for i, component in enumerate(query_components):
            if 'count=' in component:
                query_components[i] = 'count=' + str(self.max_dataset)
            elif self.get_pagination_mechanism() + '=' in component:
                query_components[i] = self.get_pagination_mechanism() + '=' + str(self.start_index)
            elif 'orderby=' in component and self.orderby:
                query_components[i] = 'orderby=' + self.orderby
        query = '&'.join(query_components)
        self.current_url = '{}?{}'.format(base_url, query)
    
    def update_index(self, index):
        min_index = self.get_mininum_pagination_value()
        if self.get_pagination_mechanism() == 'startIndex':
            self.start_index = int(index) if index else min_index

    def increment_index(self):
        if self.get_pagination_mechanism() == 'startIndex':
            self.start_index += 1

    def get_index(self):
        return self.start_index

    def get_results(self):
        if self.username and self.password:
            r = requests.get(self.current_url,
                            auth=HTTPBasicAuth(self.username, self.password),
                            verify=False, timeout=self.timeout)
        else:
            r = requests.get(self.current_url,
                            verify=False, timeout=self.timeout)

        if r.status_code != 200:
            return {'status_code': r.status_code,
                    'message': r.text}
        else:
            if 'json' in self.results_format:
                results = json.loads(r.text)
            elif 'xml' in self.results_format:
                results = xmltodict.parse(r.text)
            
            dataset_tag = self._collection["dataset_tag"]["relative_location"]
            dataset_path = dataset_tag.split(',')

            if len(dataset_path) == 0:
                return {'status_code': 400,
                        'message': 'Entries in query response did not return in list format.'}
            else:
                return get_entries(results, dataset_path)
    
    def get_name_path(self):
        mandatory_fields = self.get_mandatory_fields()
        return mandatory_fields["name"]

    def get_mandatory_fields(self):
        return self._collection["mandatory_fields"]

    def get_resource_fields(self):
        return self._collection["resources"]

