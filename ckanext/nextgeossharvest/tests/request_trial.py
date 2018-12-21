import requests
from requests.exceptions import Timeout
import time

from bs4 import BeautifulSoup as Soup
from requests.auth import HTTPBasicAuth
import requests
from requests.exceptions import Timeout
import uuid
import logging
from string import Template

log = logging.getLogger(__name__)

def normalize_names(item_node):
        """
        Return a dictionary of metadata fields with normalized names.

        The Sentinel entries are composed of metadata elements with names
        corresponding to the contents of name_elements and title, link,
        etc. elements. We can just extract all the metadata elements at
        once and rename them in one go.

        Note that elements like `ingestiondate`, which are included in the
        scihub results, will not be added to item as they are not part of the
        list of elements added in the original version.
        """
        spatial_dict={'gmd:westboundlongitude':None, 'gmd:eastboundlongitude':None,
                      'gmd:southboundlatitude':None, 'gmd:northboundlatitude':None}
        
        
        normalized_names = {
            # Since Micka Catalogue datasets refer to an entire day, there is only one datastamp,
            # Thus, this value will be added to both StartTime and StopTime, and later pos-processed
            'gmd:datestamp': 'StartTime',
            'gmd:westboundlongitude': 'spatial',
            'gmd:eastboundlongitude': 'spatial',
            'gmd:southboundlatitude': 'spatial',
            'gmd:northboundlatitude': 'spatial',         
            'gmd:fileidentifier': 'identifier',
            'gmd:title': 'title',
            'gmd:abstract': 'notes'
        }
        item = {'spatial': spatial_dict}

        #print item_node
        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                #print subitem_node.name
                key = normalized_names[subitem_node.name]
                #print key
                if key:
                    if key is 'spatial':
                        item[key][subitem_node.name] = subitem_node.text
                    elif key in ['identifier','summary', 'title']:
                        item[key] = subitem_node.text
                    else:
                        item[key] = subitem_node.text.lower()
                    
        
        # Since the spatial field is composed by 4 values, if either of the values is None ()
        for key in item['spatial']:
            if item['spatial'][key] is None:
                del item['spatial']
                break
                
        if ('StartTime' in item) and ('StopTime' not in item):
            item['StopTime'] = item['StartTime']
            
        if not item['StartTime'].endswith('Z'):
            item['StartTime'] += 'T00:00:00.000Z'
            
        if not item['StopTime'].endswith('Z'):
            item['StopTime'] += 'T23:59:59.999Z'
            
        return item

def get_next_url(harvest_url, records_returned, next_record, limit = 100):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """
        if next_record is not '0' and eval(records_returned) == limit:
            splitted_url = harvest_url.split('StartPosition')
            next_url = splitted_url[0] + 'StartPosition=' + next_record
            return next_url
        else:
            return None

harvest_url = 'https://micka.lesprojekt.cz/csw?service=CSW&version=2.0.2&request=GetRecords&TYPENAMES=record&sortby=title:A&MaxRecords=100&StartPosition=1'

entries = []
collection_metadata = []
resources_list = {}

for a in range(1):
    r = requests.get(harvest_url)
    
    soup = Soup(r.content, 'lxml')
    #print soup
    next_info = soup.find('csw:searchresults', elementset="summary")
    records_returned = next_info['numberofrecordsreturned']
    next_record = next_info['nextrecord']
    harvest_url = get_next_url(harvest_url, records_returned, next_record, 100)
    harest_url = None
    #print harvest_url
    harvest_line = 0
    thumbnail_count = 0
    
    for entry in soup.find_all('gmd:md_metadata'):
        #print entry
        content = entry.encode()
        #print content
        harvest_line += 1
        identifier = entry.find(['gmd:fileidentifier','gco:characterstring']).text.lower()
        
        #if identifier.startswith('olu'):
        soup = Soup(content, 'lxml')
        resources = entry.find('gmd:md_browsegraphic')
        
        if resources:
            name = resources.find('gmd:filename').text
            if name:
                #print name
                thumbnail_count += 1
            else:
                print 'NONE'
                
        print '{} harvest objects, and only {} had thumbnail'.format(harvest_line, thumbnail_count)   
print resources_list
    
Iwant = False
if Iwant:
    entries = []
    collection_metadata = []
    for entry in soup.find_all('gmd:md_metadata'):
        #print entry
        content = entry.encode()
        #print content
        
        identifier = entry.find(['gmd:fileidentifier','gco:characterstring']).text.lower()
        #print identifier
        
        guid = unicode(uuid.uuid4())
        #print guid
        
        restart_date = entry.find(['gmd:datestamp', 'gco:date']).text.lower()
        entries.append({'content': content, 'identifier': identifier,
                                'guid': guid, 'restart_date': restart_date})
        
        #print entries
        
        #item = normalize_names(entry)
        #collection_metadata.append(item)
        
        soup = Soup(content, 'lxml')
    
        # Create an item dictionary and add metadata with normalized names.
        item = normalize_names(soup)
    
        # If there's a spatial element, convert it to GeoJSON
        # Remove it if it's invalid
        
        # To be passed to class function
        spatial_data = item.pop('spatial', None)
        if spatial_data:
            template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
            coords_NW = [eval(spatial_data['gmd:westboundlongitude']), eval(spatial_data['gmd:northboundlatitude'])]
            coords_NE = [eval(spatial_data['gmd:eastboundlongitude']), eval(spatial_data['gmd:northboundlatitude'])]
            coords_SE = [eval(spatial_data['gmd:eastboundlongitude']), eval(spatial_data['gmd:southboundlatitude'])]
            coords_SW = [eval(spatial_data['gmd:westboundlongitude']), eval(spatial_data['gmd:southboundlatitude'])]
            coords_list = [coords_NW, coords_NE, coords_SE, coords_SW, coords_NW]
           
            geojson = template.substitute(coords_list=coords_list)
            item['spatial'] = geojson
         
        item['collection_name'] = item['title']  # noqa: E501
        item['collection_description'] = item['notes']  # noqa: E501
        item['collection_id'] = (item['title'].replace(' ', '_')).upper()
            
        for key in item:
            print item[key]
        
        
        #print('Spatial Field is {}'.format(item['spatial']))
        
        print item
    

              
                
                
                
                