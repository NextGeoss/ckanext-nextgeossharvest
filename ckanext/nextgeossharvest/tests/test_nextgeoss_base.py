"""Tests for nextgeoss_base.py."""

import json

from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class TestConvertToGeoJSON(object):
    """Tests for the _convert_to_geojson() method."""

    def test_detect_polygon(self):
        coords_1 = 'POLYGON ((-12.947799 27.343195,-15.513319 27.760723,-15.196670 29.384781,-12.589683 28.970003,-12.947799 27.343195))'  # noqa: E501
        coords_2 = 'POINT(-12.947799 27.343195)'
        coords_3 = 'POLYGON ((10 10, 10))'

        assert NextGEOSSHarvester()._convert_to_geojson(coords_1) is not None
        assert NextGEOSSHarvester()._convert_to_geojson(coords_2) is None
        assert NextGEOSSHarvester()._convert_to_geojson(coords_3) is None

    def test_is_geojson(self):
        coords = 'POLYGON ((-12.947799 27.343195,-15.513319 27.760723,-15.196670 29.384781,-12.589683 28.970003,-12.947799 27.343195))'  # noqa: E501
        geojson = NextGEOSSHarvester()._convert_to_geojson(coords)

        assert json.loads(geojson)
        geo_dict = json.loads(geojson)
        assert geo_dict['type'] == 'Polygon'
        assert geo_dict['coordinates'][0] == [[-12.947799, 27.343195], [-15.513319, 27.760723], [-15.19667, 29.384781], [-12.589683, 28.970003], [-12.947799, 27.343195]]  # noqa: E501
