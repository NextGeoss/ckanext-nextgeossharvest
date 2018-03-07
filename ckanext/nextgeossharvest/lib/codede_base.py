# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup as Soup

from ckanext.harvest.harvesters.base import HarvesterBase

from ckanext.nextgeossharvest.lib.esa_base import SentinelHarvester


class CODEDEBase(SentinelHarvester, HarvesterBase):
    """
    Base class containing methods required to make the CODE-DE harvester work.

    The _parse_content() method overrides that of SentinelHarvester.

    It may make sense to move some of SentinelHarvester's methods to the
    more general NextGEOSSHarvester class, in which case we'll need to update
    this module.
    """

    def _get_from_summary(self, term):
        """
        Pluck the value associated with a term out of the summary.

        The summary is an HTML table with some messy (but consistent)
        formatting, so we can use this method to effectively replicate
        x.find(term).text from Beautiful Soup.
        """
        node = self.summary.find(string=term)
        if node:
            return node.parent.parent.next_sibling.next_sibling.text
        else:
            return None

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        soup = Soup(content, 'lxml')
        self.summary = soup.find('summary')

        item = {}
        family_name = self._get_from_summary('Instrument Short Name')
        if family_name == 'SAR':
            instrument_name = 'Synthetic Aperture Radar'
        elif family_name == 'MSI':
            instrument_name = 'Multi-Spectral Instrument'
        else:
            instrument_name = None
        item['InstrumentFamilyName'] = family_name
        item['InstrumentName'] = instrument_name
        item['FamilyName'] = self._get_from_summary('Platform Short Name')
        item['Swath'] = self._get_from_summary('Swath Identifier')
        item['InstrumentMode'] = self._get_from_summary('Sensor Operational Mode')  # noqa: E501
        item['OrbitDirection'] = self._get_from_summary('Orbit Direction')
        cloud_coverage = self._get_from_summary('Cloud Coverage') or self._get_from_summary('Cloud Coverage ')  # noqa: E501
        if cloud_coverage:
            item['CloudCoverage'] = cloud_coverage.split(' ')[0]  # Drop % sign
        times = soup.find('dc:date').text.split('/')
        item['StartTime'] = times[0]
        item['StopTime'] = times[1]
        identifier = self.normalize_identifier(soup.find('dc:identifier').text)
        item['identifier'] = identifier
        item['name'] = identifier.lower()

        # TODO: This reformatting process should be a method ##################
        bad_poly = soup.find('georss:polygon').text.split(' ')
        odd_poly = bad_poly[::2]
        even_poly = bad_poly[1::2]
        zipped_poly = zip(odd_poly, even_poly)
        # # Also have to reorder the coordinates...
        list_poly = ['{} {}'.format(x[1], x[0]) for x in zipped_poly]
        polygon = ','.join(list_poly)
        wkt_polygon = 'POLYGON (({}))'.format(polygon)
        # End TODO ############################################################

        geojson = self._convert_to_geojson(wkt_polygon)
        if geojson:
            item['spatial'] = geojson

        # Thumbnail, alternative and enclosure
        enclosure = soup.find('link', rel='enclosure')['href']
        alternative = soup.find('link', rel='alternate')['href']
        thumbnail_link = None  # TODO: Check for working icon links
        item['codede_download_url'] = enclosure
        item['codede_product_url'] = alternative
        item['codede_thumbnail'] = thumbnail_link

        # Convert size (298.74MB to an integer representing bytes)
        size = self._get_from_summary('Download Size') or self._get_from_summary('Download Size ')  # noqa: E501
        if size:
            item['size'] = int(float(size.split('MB')[0]) * 1000000)

        # Figure out what collection the product belongs to
        item = self._add_collection(item)

        item['title'] = item['collection_name']
        item['notes'] = item['collection_description']

        # Add some tags automatically
        item['tags'] = self._get_tags_for_dataset(item)

        # Add time range metadata that's not tied to product-specific fields
        # like StartTime so that we can filter by a dataset's time range
        # without having to cram other kinds of temporal data into StartTime
        # and StopTime fields, etc.
        item['timerange_start'] = item['StartTime']
        item['timerange_end'] = item['StopTime']

        return item

    def normalize_identifier(self, code_de_ident):
        """Return the real identifier without the extra CODE-DE cruft."""
        return code_de_ident.split('/')[1]

    def _get_end_date(self, dates):
        """Return the end time from a dc:date element."""
        return dates.split('/')[0]
