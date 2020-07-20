COLLECTION = {
    "S1_ACD_SATCEN_BETTER": {
        "collection_id": "S1_ACD_SATCEN_BETTER",
        "collection_name": "Sentinel-1 ACD BETTER project",
        "collection_description": "SAR sensors are a valuable tool to detect changes over man-made and natural structures: especially in equatorial regions, where clouds are present throughout the whole year thus limiting the use of optical sensors, SAR sensors could be used as complementary source of information. This collection provides RGB composites of the backscatter of two Sentinel-1 images at different dates, a ready-to-analysis product for the identification of changes. The RGB structure is R: image1 backscatter, G: image! Backscatter, B: image2 backscatter. Yellow pixels means that the backscatter has decreased while blue pixel means that it has increased.",
        "dataset_tag": {
            "relative_location": "features"
        },
        "mandatory_fields": {
            "title": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "notes": {
                "relative_location": "properties,title"
            },
            "identifier": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "name": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "spatial": {
                "location": {
                    "relative_location": "properties,spatial"
                },
                "parse_function": "WKT"
            },
            "timerange_start": {
                "location": {
                    "relative_location": "properties,title"
                },
                "parse_function": "custom"
            },
            "timerange_end":{
                "location": {
                    "relative_location": "properties,title"
                },
                "parse_function": "custom"
            },
        },
        "resources": [
            {
                "url": {
                    "field_type": "path",
                    "location":{
                        "relative_location": "properties,links,@href",
                        "fixed_attributes": [{
                            "attribute": "@rel",
                            "value": "enclosure"
                        },
                        {
                            "attribute": "@type",
                            "value": "image/tiff"
                        }]
                    }
                },
                "description": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Hyperlink for GeoTIFF product download"
                    }
                },
                "mimetype": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "image/tiff"
                    }
                },
                "format": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "GeoTIFF"
                    }
                },
                "name": {
                    "field_type": "path",
                    "location": {
                        "relative_location": "properties,links,@title",
                        "fixed_attributes": [{
                            "attribute": "@rel",
                            "value": "enclosure"
                        },
                        {
                            "attribute": "@type",
                            "value": "image/tiff"
                        }]
                    }
                }
            },
            {
                "url": {
                    "field_type": "path",
                    "location":{
                        "relative_location": "properties,offering,operation,@href",
                        "fixed_attributes": [{
                            "attribute": "@code",
                            "value": "GetMap"
                        }]
                    }
                },
                "description": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Hyperlink for the product quicklook"
                    }
                },
                "mimetype": {
                    "field_type": "path",
                    "location": {
                        "relative_location": "properties,offering,operation,@type",
                        "fixed_attributes": [{
                            "attribute": "@code",
                            "value": "GetMap"
                        }]
                    }
                },
                "format": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "PNG"
                    }
                },
                "name": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Product Quicklook"
                    }
                }
            }
        ]
    },
    "S2_MINERAL_INDEX_SATCEN_BETTER": {
        "collection_id": "S2_MINERAL_INDEX_SATCEN_BETTER",
        "collection_name": "Sentinel-1 ACD BETTER project",
        "collection_description": "SAR sensors are a valuable tool to detect changes over man-made and natural structures: especially in equatorial regions, where clouds are present throughout the whole year thus limiting the use of optical sensors, SAR sensors could be used as complementary source of information. This collection provides RGB composites of the backscatter of two Sentinel-1 images at different dates, a ready-to-analysis product for the identification of changes. The RGB structure is R: image1 backscatter, G: image! Backscatter, B: image2 backscatter. Yellow pixels means that the backscatter has decreased while blue pixel means that it has increased.",
        "dataset_tag": {
            "relative_location": "features"
        },
        "mandatory_fields": {
            "title": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "notes": {
                "relative_location": "properties,title"
            },
            "identifier": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "name": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "spatial": {
                "location": {
                    "relative_location": "properties,spatial"
                },
                "parse_function": "WKT"
            },
            "timerange_start": {
                "location": {
                    "relative_location": "properties,date"
                },
                "parse_function": "complete_slash"
            },
            "timerange_end":{
                "location": {
                    "relative_location": "properties,date"
                },
                "parse_function": "complete_slash"
            },
        },
        "resources": [
            {
                "url": {
                    "field_type": "path",
                    "location":{
                        "relative_location": "properties,links,@href",
                        "fixed_attributes": [{
                            "attribute": "@rel",
                            "value": "enclosure"
                        },
                        {
                            "attribute": "@type",
                            "value": "image/tiff"
                        }]
                    }
                },
                "description": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Hyperlink for GeoTIFF product download"
                    }
                },
                "mimetype": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "image/tiff"
                    }
                },
                "format": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "GeoTIFF"
                    }
                },
                "name": {
                    "field_type": "path",
                    "location": {
                        "relative_location": "properties,links,@title",
                        "fixed_attributes": [{
                            "attribute": "@rel",
                            "value": "enclosure"
                        },
                        {
                            "attribute": "@type",
                            "value": "image/tiff"
                        }]
                    }
                }
            },
            {
                "url": {
                    "field_type": "path",
                    "location":{
                        "relative_location": "properties,offering,operation,@href",
                        "fixed_attributes": [{
                            "attribute": "@code",
                            "value": "GetMap"
                        }]
                    }
                },
                "description": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Hyperlink for the product quicklook"
                    }
                },
                "mimetype": {
                    "field_type": "path",
                    "location": {
                        "relative_location": "properties,offering,operation,@type",
                        "fixed_attributes": [{
                            "attribute": "@code",
                            "value": "GetMap"
                        }]
                    }
                },
                "format": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "PNG"
                    }
                },
                "name": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Product Quicklook"
                    }
                }
            }
        ]
    },
    "S2_VEGETAION_MASK_SATCEN_BETTER": {
        "collection_id": "S2_VEGETAION_MASK_SATCEN_BETTER",
        "collection_name": "Sentinel-2 Vegetation mask BETTER project",
        "collection_description": "A product with six bands containing the following information: Band 1 (NDVI_class); Band 2 (BSI_class); Band 3 (Mask); Band 4 (Vegetation_mask); Band 5 (BSI_mask); Band 6 (NDVI_mask). It has been computed using Sentinel-2 images.",
        "dataset_tag": {
            "relative_location": "features"
        },
        "mandatory_fields": {
            "title": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "notes": {
                "relative_location": "properties,title"
            },
            "identifier": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "name": {
                "relative_location": "properties,links,@ref",
                "fixed_attributes": [{
                    "attribute": "@rel",
                    "value": "enclosure"
                },
                {
                    "attribute": "@type",
                    "value": "image/tiff"
                }]
            },
            "spatial": {
                "location": {
                    "relative_location": "properties,geometry"
                },
                "parse_function": "GeoJSON"
            },
            "timerange_start": {
                "location": {
                    "relative_location": "properties,date"
                },
                "parse_function": "complete_slash"
            },
            "timerange_end": {
                "location": {
                    "relative_location": "properties,date"
                },
                "parse_function": "complete_slash"
            },
        },
        "resources": [
            {
                "url": {
                    "field_type": "path",
                    "location":{
                        "relative_location": "properties,links,@href",
                        "fixed_attributes": [{
                            "attribute": "@rel",
                            "value": "enclosure"
                        },
                        {
                            "attribute": "@type",
                            "value": "image/tiff"
                        }]
                    }
                },
                "description": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Hyperlink for GeoTIFF product download"
                    }
                },
                "mimetype": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "image/tiff"
                    }
                },
                "format": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "GeoTIFF"
                    }
                },
                "name": {
                    "field_type": "path",
                    "location": {
                        "relative_location": "properties,links,@title",
                        "fixed_attributes": [{
                            "attribute": "@rel",
                            "value": "enclosure"
                        },
                        {
                            "attribute": "@type",
                            "value": "image/tiff"
                        }]
                    }
                }
            },
            {
                "url": {
                    "field_type": "path",
                    "location":{
                        "relative_location": "properties,offering,operation,@href",
                        "fixed_attributes": [{
                            "attribute": "@code",
                            "value": "GetMap"
                        }]
                    }
                },
                "description": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Hyperlink for the product quicklook"
                    }
                },
                "mimetype": {
                    "field_type": "path",
                    "location": {
                        "relative_location": "properties,offering,operation,@type",
                        "fixed_attributes": [{
                            "attribute": "@code",
                            "value": "GetMap"
                        }]
                    }
                },
                "format": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "PNG"
                    }
                },
                "name": {
                    "field_type": "freeText",
                    "location": {
                        "relative_location": "Product Quicklook"
                    }
                }
            }
        ]
    }
}