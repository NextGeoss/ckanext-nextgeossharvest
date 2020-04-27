# -*- coding: utf-8 -*-

COLLECTION = {
    "SCENT_DANUBE_IMAGE": {
        "collection_name": "SCENT Danube Image",
        "collection_description": "TBD",
        "collection_typename": "geomesa:danubeImageMetadata",
        "tag_typename": "geomesa:danubeTagMetadata",
        "property_ignore_list": ["URI"],
        "url_key": "URI"
    },
    "SCENT_DANUBE_VIDEO": {
        "collection_name": "SCENT Danube Video",
        "collection_description": "TBD",
        "collection_typename": "geomesa:danubeVideoMetadata",
        "property_ignore_list": ["crowdURI"],
        "url_key": "crowdURI"
    },
    "SCENT_DANUBE_MOISTURE": {
        "collection_name": "SCENT Danube Moisture",
        "collection_description": "TBD",
        "collection_typename": "geomesa:danubeMoistureMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel", "trustLevel"]
    },
    "SCENT_DANUBE_TEMPERATURE": {
        "collection_name": "SCENT Danube Temperature",
        "collection_description": "TBD",
        "collection_typename": "geomesa:danubeTempMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel", "trustLevel"]
    },
    "SCENT_KIFISOS_IMAGE": {
        "collection_name": "SCENT Kifisos Image",
        "collection_description": "TBD",
        "collection_typename": "geomesa:kifisosImageMetadata",
        "tag_typename": "geomesa:kifisosTagMetadata",
        "property_ignore_list": ["URI"],
        "url_key": "URI"
    },
    "SCENT_KIFISOS_VIDEO": {
        "collection_name": "SCENT Kifisos Video",
        "collection_description": "TBD",
        "collection_typename": "geomesa:kifisosVideoMetadata",
        "property_ignore_list": ["crowdURI"],
        "url_key": "crowdURI"
    },
    "SCENT_KIFISOS_MOISTURE": {
        "collection_name": "SCENT Kifisos Moisture",
        "collection_description": "TBD",
        "collection_typename": "geomesa:kifisosMoistureMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel"]
    },
    "SCENT_KIFISOS_TEMPERATURE": {
        "collection_name": "SCENT Kifisos Temperature",
        "collection_description": "TBD",
        "collection_typename": "geomesa:kifisosTempMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel"]
    }
}