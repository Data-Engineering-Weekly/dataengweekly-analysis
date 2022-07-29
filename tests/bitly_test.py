
from dataengweekly_analysis.bitly import get_country_map
from dataengweekly_analysis.data_path import get_csv_path


def test_country_metadata_map():
    country_map = get_country_map()
    assert 'United Kingdom' == country_map['GB']
    assert 'Russia' == country_map['RU']

