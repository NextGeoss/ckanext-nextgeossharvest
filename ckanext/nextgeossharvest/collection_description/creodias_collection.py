COLLECTION = {
    "Sentinel-1 Level-1 (SLC)": {
        "collection_name": "Sentinel-1 Level-1 (SLC)",
        "collection_description": "The Sentinel-1 Level-1 Single Look Complex (SLC) products consist of focused SAR data geo-referenced using orbit and attitude data from the satellite and provided in zero-Doppler slant-range geometry. The products include a single look in each dimension using the full TX signal bandwidth and consist of complex samples preserving the phase information.",
        "collection_search": "urn:eop:CREODIAS:Sentinel1Level1SLC",
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
            },
            "spatial":{
                "path": [
                    [
                        {
                            "key": "geometry",
                            "fixed_attributes": []
                        }
                    ]
                ],
                "parsing_function": "GeoJSON"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Product Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the product from CREODIAS. NOTE: DOWNLOAD REQUIRES TOKEN"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"services",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"download",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"url",
                            "fixed_attributes":[]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Thumbnail Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the Thumbnail from CREODIAS"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key": "thumbnail",
                            "fixed_attributes": []
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " Extra Resource"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download Extra Resource"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
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
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "instrument",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "instrument",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "platform",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "platform",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productType",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "productType",
                "solr": False
            }
        ]
    },
    "Sentinel-1 Level-1 (GRD)": {
        "collection_name": "Sentinel-1 Level-1 (GRD)",
        "collection_description": "The Sentinel-1 Level-1 Ground Range Detected (GRD) products consist of focused SAR data that has been detected, multi-looked and projected to ground range using an Earth ellipsoid model. Phase information is lost. The resulting product has approximately square resolution pixels and square pixel spacing with reduced speckle at the cost of reduced geometric resolution.",
        "collection_search": "urn:eop:CREODIAS:Sentinel1Level1GRD",
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
            },
            "spatial":{
                "path": [
                    [
                        {
                            "key": "geometry",
                            "fixed_attributes": []
                        }
                    ]
                ],
                "parsing_function": "GeoJSON"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Product Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the product from CREODIAS. NOTE: DOWNLOAD REQUIRES TOKEN"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"services",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"download",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"url",
                            "fixed_attributes":[]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Thumbnail Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the Thumbnail from CREODIAS"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key": "thumbnail",
                            "fixed_attributes": []
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " Extra Resource"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download Extra Resource"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
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
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "instrument",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "instrument",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "platform",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "platform",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productType",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "productType",
                "solr": False
            }
        ]
    },
    "Sentinel-1 Level-2 (OCN)": {
        "collection_name": "Sentinel-1 Level-2 (OCN)",
        "collection_description": "The Sentinel-1 Level-2 OCN products include components for Ocean Swell spectra (OSW) providing continuity with ERS and ASAR WV and two new components: Ocean Wind Fields (OWI) and Surface Radial Velocities (RVL). The OSW is a two-dimensional ocean surface swell spectrum and includes an estimate of the wind speed and direction per swell spectrum. The OWI is a ground range gridded estimate of the surface wind speed and direction at 10 m above the surface derived from internally generated Level-1 GRD images of SM, IW or EW modes. The RVL is a ground range gridded difference between the measured Level-2 Doppler grid and the Level-1 calculated geometrical Doppler.",
        "collection_search": "urn:eop:CREODIAS:Sentinel1Level2OCN",
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
            },
            "spatial":{
                "path": [
                    [
                        {
                            "key": "geometry",
                            "fixed_attributes": []
                        }
                    ]
                ],
                "parsing_function": "GeoJSON"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Product Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the product from CREODIAS. NOTE: DOWNLOAD REQUIRES TOKEN"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"services",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"download",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"url",
                            "fixed_attributes":[]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Thumbnail Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the Thumbnail from CREODIAS"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key": "thumbnail",
                            "fixed_attributes": []
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " Extra Resource"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download Extra Resource"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
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
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "instrument",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "instrument",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "platform",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "platform",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productType",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "productType",
                "solr": False
            }
        ]
    },
    "Sentinel-2 Level-2A": {
        "collection_name": "Sentinel-2 Level-2A",
        "collection_description": "The Sentinel-2 Level-2A products are Bottom-of-atmosphere reflectances in cartographic geometry (prototype product). These products are generated using Sentinel-2 Toolbox and the data volume is 600MB for each 100x100 km2.",
        "collection_search": "urn:eop:CREODIAS:Sentinel2Level2A",
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
            },
            "spatial":{
                "path": [
                    [
                        {
                            "key": "geometry",
                            "fixed_attributes": []
                        }
                    ]
                ],
                "parsing_function": "GeoJSON"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Product Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the product from CREODIAS. NOTE: DOWNLOAD REQUIRES TOKEN"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"services",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"download",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"url",
                            "fixed_attributes":[]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Thumbnail Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the Thumbnail from CREODIAS"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key": "thumbnail",
                            "fixed_attributes": []
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " Extra Resource"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download Extra Resource"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
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
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "instrument",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "instrument",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "platform",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "platform",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productType",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "productType",
                "solr": False
            }
        ]
    },
    "Sentinel-2 Level-1C": {
        "collection_name": "Sentinel-2 Level-1C",
        "collection_description": "The Sentinel-2 Level-1C products are Top-of-atmosphere reflectances in cartographic geometry. These products are systematically generated and the data volume is 500MB for each 100x100 km2.",
        "collection_search": "urn:eop:CREODIAS:Sentinel2Level1C",
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
            },
            "spatial":{
                "path": [
                    [
                        {
                            "key": "geometry",
                            "fixed_attributes": []
                        }
                    ]
                ],
                "parsing_function": "GeoJSON"
            }
        },
        "resources": [
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Product Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the product from CREODIAS. NOTE: DOWNLOAD REQUIRES TOKEN"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"services",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"download",
                            "fixed_attributes":[]
                        },
                        {
                            "key":"url",
                            "fixed_attributes":[]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "Thumbnail Download from CREODIAS"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download the Thumbnail from CREODIAS"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
                        {
                            "key": "thumbnail",
                            "fixed_attributes": []
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": " Extra Resource"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download Extra Resource"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key":"properties",
                            "fixed_attributes":[]
                        },
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
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "instrument",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "instrument",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "platform",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "platform",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "productType",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "productType",
                "solr": False
            }
        ]
    },
}