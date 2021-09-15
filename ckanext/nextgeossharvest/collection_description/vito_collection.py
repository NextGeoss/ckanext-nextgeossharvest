COLLECTION = {
    "SENTINEL_2_TOC_V2": {
        "collection_name": "Sentinel-2 TOC V2",
        "collection_description": "L2A atmospheric corrected Top-Of-Canopy (TOC) products V2, generated using the Sen2COR processing tool.",
        "collection_search": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "previews",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "tileId",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "tile_id",
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
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "CGS_S1_GRD_L1": {
        "collection_name": "CGS S1 GRD L1",
        "collection_description": "S1 datasets processed on VITO premises. Level-1 Single Look Complex (SLC) products consist of focused SAR data, geo-referenced using orbit and attitude data from the satellite, and provided in slant-range geometry. Slant range is the natural radar range observation coordinate, defined as the line-of-sight from the radar to each reflecting object. The products are in zero-Doppler orientation where each row of pixels represents points along a line perpendicular to the sub-satellite track.",
        "collection_search": "urn:eop:VITO:CGS_S1_GRD_L1",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "Ground Range Detected product"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download GRD product"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "application/zip"
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "operationalMode",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "operational_mode",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "polarisationChannels",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "polarisation_channels",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "polarisationMode",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "polarisation_mode",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "CGS_S1_SLC_L1": {
        "collection_name": "CGS S1 SLC L1",
        "collection_description": "S1 datasets processed on VITO premises. Level-1 Single Look Complex (SLC) products consist of focused SAR data, geo-referenced using orbit and attitude data from the satellite, and provided in slant-range geometry. Slant range is the natural radar range observation coordinate, defined as the line-of-sight from the radar to each reflecting object. The products are in zero-Doppler orientation where each row of pixels represents points along a line perpendicular to the sub-satellite track.",
        "collection_search": "urn:eop:VITO:CGS_S1_SLC_L1",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "Single Look Complex product"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download SLC product"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "application/zip"
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "operationalMode",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "operational_mode",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "polarisationChannels",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "polarisation_channels",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "polarisationMode",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "polarisation_mode",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "CGS_S1_GRD_SIGMA0_L1": {
        "collection_name": "CGS S1 GRD SIGMA0 L1",
        "collection_description": "The Sigma0 product describes how much of the radar signal that was sent out by Sentinel-1 is reflected back to the sensor, and depends on the characteristics of the surface. This product is derived from the L1-GRD product. Typical SAR data processing, which produces level 1 images such as L1-GRD product, does not include radiometric corrections and significant radiometric bias remains. Therefore, it is necessary to apply the radiometric correction to SAR images so that the pixel values of the SAR images truly represent the radar backscatter of the reflecting surface. The radiometric correction is also necessary for the comparison of SAR images acquired with different sensors, or acquired from the same sensor but at different times, in different modes, or processed by different processors. For this Sigma0 product, radiometric calibration was performed using a specific Look Up Table (LUT) that is provided with each original GRD product. This LUT applies a range-dependent gain including the absolute calibration constant, in addition to a constant offset. Next to calibration, also orbit correction, border noise removal, thermal noise removal, and range doppler terrain correction steps were applied during production of Sigma0. The terrain correction step is intended to compensate for distortions due to topographical variations of the scene and the tilt of the satellite sensor, so that the geometric representation of the image will be as close as possible to the real world. The Level1 GRD product can be useful for Land monitoring and Emergency management.",
        "collection_search": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "VH"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "VH"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "VH"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "VV"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "VV"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "VV"
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "name": {
                    "field_type": "freeText",
                    "freeText": "angle"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "angle"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "title",
                                    "value": "angle"
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "operationalMode",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "operational_mode",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "polarisationChannels",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "polarisation_channels",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "polarisationMode",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "polarisation_mode",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "TERRASCOPE_SENTINEL_2_LAI_V2": {
        "collection_name": "Terrascope Sentinel-2 LAI V2",
        "collection_description": "LAI was defined by Committee of the Earth Observation System (CEOS) as half the developed area of the convex hull wrapping the green canopy elements per unit horizontal ground. This definition allows accounting for elements which are not flat such as needles or stems. LAI is strongly non linearly related to reflectance. Therefore, its estimation from remote sensing observations will be scale dependant over heterogeneous landscapes. When observing a canopy made of different layers of vegetation, it is therefore mandatory to consider all the green layers. This is particularly important for forest canopies where the understory may represent a very significant contribution to the total canopy LAI. The derived LAI corresponds therefore to the total green LAI, including the contribution of the green elements of the understory. The resulting SENTNEL LAI products are relatively consistent with the actual LAI for low LAI values and \u2018non-forest\u2019 surfaces; while for forests, particularly for needle leaf types, significant departures with the True LAI are expected.",
        "collection_search": "urn:eop:VITO:TERRASCOPE_S2_LAI_V2",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                    "freeText": "Inspire Metadata"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Access to product original metadata description "
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "Leaf Area Index product"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download LAI product"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "previews",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
                                {
                                    "key": "type",
                                    "value": "image/tiff"
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "tileId",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "tile_id",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "additionalAttributes",
                        "fixed_attributes": []
                    },
                    {
                        "key": "resolution",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "resolution",
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
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "TERRASCOPE_SENTINEL_2_NDVI_V2": {
        "collection_name": "Terrascope Sentinel-2 NDVI V2",
        "collection_description": "The SENTINEL-2 Normalized Difference Vegetation Index (NDVI) is a proxy to quantify the vegetation amount. It is defined as NDVI=(NIR-Red)/(NIR+Red) where NIR corresponds to the reflectance in the near infrared band , and Red to the reflectance in the red band. It is closely related to FAPAR and is little scale dependant.",
        "collection_search": "urn:eop:VITO:TERRASCOPE_S2_NDVI_V2",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "Normalized Difference Vegetation Index product"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download NDVI product"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "previews",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "tileId",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "tile_id",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "additionalAttributes",
                        "fixed_attributes": []
                    },
                    {
                        "key": "resolution",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "resolution",
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
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "TERRASCOPE_SENTINEL_2_FAPAR_V2": {
        "collection_name": "Terrascope Sentinel-2 FAPAR V2",
        "collection_description": "FAPAR corresponds to the fraction of photosynthetically active radiation absorbed by the canopy.The FAPAR value results directly from the radiative transfer model in the canopy which is computed instantaneously. It depends on canopy structure, vegetation element optical properties and illumination conditions. FAPAR is very useful as input to a number of primary productivity models which run at the daily time step. Consequently, the product definition should correspond to the daily integrated FAPAR value that can be approached by computation of the clear sky daily integrated FAPAR values as well as the FAPAR value computed for diffuse conditions. The SENTINEL 2 FAPAR product corresponds to the instantaneous black-sky around 10:15 which is a close approximation of the daily integrated black-sky FAPAR value. The FAPAR refers only to the green parts of the canopy.FAPAR corresponds to the fraction of photosynthetically active radiation absorbed by the canopy.The FAPAR value results directly from the radiative transfer model in the canopy which is computed instantaneously. It depends on canopy structure, vegetation element optical properties and illumination conditions. FAPAR is very useful as input to a number of primary productivity models which run at the daily time step. Consequently, the product definition should correspond to the daily integrated FAPAR value that can be approached by computation of the clear sky daily integrated FAPAR values as well as the FAPAR value computed for diffuse conditions. The SENTINEL 2 FAPAR product corresponds to the instantaneous black-sky around 10:15 which is a close approximation of the daily integrated black-sky FAPAR value. The FAPAR refers only to the green parts of the canopy.",
        "collection_search": "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "Fraction Absorbed Photosynthetically Radiation product"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download FAPAR product"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "previews",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "tileId",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "tile_id",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "additionalAttributes",
                        "fixed_attributes": []
                    },
                    {
                        "key": "resolution",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "resolution",
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
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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
    "TERRASCOPE_SENTINEL_2_FCOVER_V2": {
        "collection_name": "Terrascope Sentinel-2 FCOVER V2",
        "collection_description": "Fraction of vegetation Cover (FCOVER) corresponds to the gap fraction for nadir direction. It is used to separate vegetation and soil in energy balance processes, including temperature and evapotranspiration. It is computed from the leaf area index and other canopy structural variables and does not depend on variables such as the geometry of illumination as compared to FAPAR. For this reason, it is a very good candidate for the replacement of classical vegetation indices for the monitoring of green vegetation. Because of the linear relationship with radiometric signal, FCOVER will be only marginally scale dependent. Note that similarly to LAI and FAPAR, only the green elements will be considered, either belonging both to the overstorey and understorey.",
        "collection_search": "urn:eop:VITO:TERRASCOPE_S2_FCOVER_V2",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "beginningDateTime",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "endingDateTime",
                        "fixed_attributes": []
                    }
                ],
                "parsing_function": "SingleDate"
            },
            "spatial": {
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "alternates",
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
                    "freeText": "Fraction of Vegetation Cover product"
                },
                "description": {
                    "field_type": "freeText",
                    "freeText": "Download FCOVER product"
                },
                "url": {
                    "field_type": "path",
                    "path": [
                        {
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "data",
                            "fixed_attributes": []
                        },
                        {
                            "key": "href",
                            "fixed_attributes": [
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
                            "key": "properties",
                            "fixed_attributes": []
                        },
                        {
                            "key": "links",
                            "fixed_attributes": []
                        },
                        {
                            "key": "previews",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
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
                        "key": "acquisitionInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "acquisitionParameters",
                        "fixed_attributes": []
                    },
                    {
                        "key": "tileId",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "tile_id",
                "solr": False
            },
            {
                "path": [
                    {
                        "key": "properties",
                        "fixed_attributes": []
                    },
                    {
                        "key": "additionalAttributes",
                        "fixed_attributes": []
                    },
                    {
                        "key": "resolution",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "resolution",
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
                        "key": "productInformation",
                        "fixed_attributes": []
                    },
                    {
                        "key": "processingCenter",
                        "fixed_attributes": []
                    }
                ],
                "field_name": "processing_center",
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