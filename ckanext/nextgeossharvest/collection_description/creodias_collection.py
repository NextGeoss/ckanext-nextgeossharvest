COLLECTION = {
    "Sentinel1": {
        "collection_name": "Sentinel1",
        "collection_description": "SENTINEL 1 Data",
        "collection_search": "urn:eop:CREODIAS:Sentinel 1",
        "dataset_tag": {
            "field_type": "path",
            "path": [
                {
                    "key": "features",
                    "fixed_attributes": []
                }
            ]
        },
        "mandatory_fields": {
            "title": {
                "field_type": "path",
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "title",
                        "fixed_attributes": []
                    }
                ]
            },
            "identifier": {
                "field_type": "path",
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "title",
                        "fixed_attributes": []
                    }
                ]
            },
            "timerange_start": {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "startDate",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "timerange_end": {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    {
                        "key": "completionDate",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Inspire Metadata"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Access to product original metadata description"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "Inspire metadata"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B05_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B05_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B05_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " TOC-B01_60M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B01_60M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B01_60M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B06_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B06_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B06_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B11_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B11_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B11_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B07_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B07_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B07_20M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B8A_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B8A_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B8A_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B03_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B03_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B03_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B04_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B04_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B04_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B12_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B12_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B12_20M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B08_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B08_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B08_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B02_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B02_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B02_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Quicklook"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Access the product quicklook"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "category",
                                    "value": "QUICKLOOK"
                                }
                            ]
                        }
                    ]
                }
            }
        ],
        "extras": [
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    
                    {
                        "key": "orbitDirection",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "orbit_direction",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    
                    {
                        "key": "orbitNumber",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "orbit_number",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    
                    {
                        "key": "relativeOrbitNumber",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "relative_orbit_number",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "cloudCover",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "cloud_cover",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "status",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "status",
                "solr": False
            }
        ]
    },

    "Sentinel2": {
        "collection_name": "Sentinel2",
        "collection_description": "SENTINEL 2 Data",
        "collection_search": "urn:eop:CREODIAS:Sentinel 2",
        "dataset_tag": {
            "field_type": "path",
            "path": [
                {
                    "key": "features",
                    "fixed_attributes": []
                }
            ]
        },
        "mandatory_fields": {
            "title": {
                "field_type": "path",
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "title",
                        "fixed_attributes": []
                    }
                ]
            },
            "identifier": {
                "field_type": "path",
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "title",
                        "fixed_attributes": []
                    }
                ]
            },
            "timerange_start": {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "startDate",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "timerange_end": {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    {
                        "key": "completionDate",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Inspire Metadata"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Access to product original metadata description"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "Inspire metadata"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B05_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B05_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B05_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " TOC-B01_60M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B01_60M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B01_60M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B06_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B06_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B06_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B11_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B11_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B11_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B07_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B07_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B07_20M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B8A_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B8A_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                },
                                {
                                    "key": "title",
                                    "value": "TOC-B8A_20M"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B03_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B03_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B03_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B04_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B04_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B04_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B12_20M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B12_20M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B12_20M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B08_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B08_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B08_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "TOC-B02_10M"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download TOC-B02_10M GeoTiff"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "TOC-B02_10M"
                                },
                                {
                                    "key": "type",
                                    "value": "image/tiff"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Quicklook"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Access the product quicklook"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "category",
                                    "value": "QUICKLOOK"
                                }
                            ]
                        }
                    ]
                }
            }
        ],
        "extras": [
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    
                    {
                        "key": "orbitDirection",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "orbit_direction",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    
                    {
                        "key": "orbitNumber",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "orbit_number",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    
                    
                    {
                        "key": "relativeOrbitNumber",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "relative_orbit_number",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "cloudCover",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "cloud_cover",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "status",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "status",
                "solr": False
            }
        ]
    }
}