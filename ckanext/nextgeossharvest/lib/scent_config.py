# -*- coding: utf-8 -*-

COLLECTION = {
    "SCENT_DANUBE_IMAGE": {
        "collection_name": "SCENT Danube Image",
        "collection_description": "It refers to metadata (textual descriptions) of images of Land Cover/Land Use (LC/LU) elements and/or of river parameters (i.e. water level) collected from volunteers in the context of H2020 Scent project (https://scent-project.eu) in Danube Delta, Romania during 2018-2019. This dataset has been initially collected through the use of Scent Explore application while being enriched by other components of the Scent toolbox such the Scent Intelligence Engine and Scent Collaborate (https://scent-project.eu/scent-toolbox).",
        "collection_typename": "geomesa:danubeImageMetadata",
        "tag_typename": "geomesa:danubeTagMetadata",
        "property_ignore_list": ["URI"],
        "url_key": "URI"
    },
    "SCENT_DANUBE_VIDEO": {
        "collection_name": "SCENT Danube Video",
        "collection_description": "It refers to metadata (textual descriptions) of videos, containing a pre-defined floating object (i.e. tennis ball) moving on the surface of a water body, collected from volunteers in the context of H2020 Scent project (https://scent-project.eu) in Danube Delta, Romania (2018-2019). The metadata contain water surface velocity measurements that have been extracted from the videos via the Water Velocity Calculation Tool consisting of innovative video processing algorithms (https://scent-project.eu/scent-toolbox).",
        "collection_typename": "geomesa:danubeVideoMetadata",
        "property_ignore_list": ["crowdURI"],
        "url_key": "crowdURI"
    },
    "SCENT_DANUBE_MOISTURE": {
        "collection_name": "SCENT Danube Moisture",
        "collection_description": "It refers to soil moisture measurements collected from volunteers through the use of portable sensors in the context of H2020 Scent project (https://scent-project.eu) in Danube Delta, Romania during 2018-2019. Scent Measure application (https://scent-project.eu/scent-toolbox) has been used in tandem with portable sensors for the collection of this dataset.",
        "collection_typename": "geomesa:danubeMoistureMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel", "trustLevel"]
    },
    "SCENT_DANUBE_TEMPERATURE": {
        "collection_name": "SCENT Danube Temperature",
        "collection_description": "It refers to air temperature measurements collected from volunteers through the use of portable sensors in the context of H2020 Scent project (https://scent-project.eu). in Danube Delta, Romania during 2018-2019. Scent Measure application (https://scent-project.eu/scent-toolbox) has been used in tandem with portable sensors for the collection of this dataset.",
        "collection_typename": "geomesa:danubeTempMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel", "trustLevel"]
    },
    "SCENT_KIFISOS_IMAGE": {
        "collection_name": "SCENT Kifisos Image",
        "collection_description": "It refers to metadata (textual descriptions) of images of Land Cover/Land Use (LC/LU) elements and/or of river parameters (i.e. water level) collected from volunteers in the context of H2020 Scent project (https://scent-project.eu) in Kifisos river basin, Greece during 2018-2019. This dataset has been initially collected through the use of Scent Explore application while being enriched by other components of the Scent toolbox such the Scent Intelligence Engine and Scent Collaborate (https://scent-project.eu/scent-toolbox).",
        "collection_typename": "geomesa:kifisosImageMetadata",
        "tag_typename": "geomesa:kifisosTagMetadata",
        "property_ignore_list": ["URI"],
        "url_key": "URI"
    },
    "SCENT_KIFISOS_VIDEO": {
        "collection_name": "SCENT Kifisos Video",
        "collection_description": "It refers to metadata (textual descriptions) of videos, containing a pre-defined floating object (i.e. tennis ball) moving on the surface of a water body, collected from volunteers in the context of H2020 Scent project (https://scent-project.eu), in Kifisos river basin, Greece (2018-2019). The metadata contain water surface velocity measurements that have been extracted from the videos via the Water Velocity Calculation Tool consisting of innovative video processing algorithms (https://scent-project.eu/scent-toolbox).",
        "collection_typename": "geomesa:kifisosVideoMetadata",
        "property_ignore_list": ["crowdURI"],
        "url_key": "crowdURI"
    },
    "SCENT_KIFISOS_MOISTURE": {
        "collection_name": "SCENT Kifisos Moisture",
        "collection_description": "It refers to soil moisture measurements collected from volunteers through the use of portable sensors in the context of H2020 Scent project (https://scent-project.eu) in Kifisos river basin, Greece during 2018-2019. Scent Measure application (https://scent-project.eu/scent-toolbox) has been used in tandem with portable sensors for the collection of this dataset",
        "collection_typename": "geomesa:kifisosMoistureMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel"]
    },
    "SCENT_KIFISOS_TEMPERATURE": {
        "collection_name": "SCENT Kifisos Temperature",
        "collection_description": "It refers to air temperature measurements collected from volunteers through the use of portable sensors in the context of H2020 Scent project (https://scent-project.eu) in Kifisos river basin, Greece during 2018-2019. Scent Measure application (https://scent-project.eu/scent-toolbox) has been used in tandem with portable sensors for the collection of this dataset.",
        "collection_typename": "geomesa:kifisosTempMetadata",
        "property_ignore_list": ["userId", "measurement_accuracy", "trustLevel"]
    }
}
