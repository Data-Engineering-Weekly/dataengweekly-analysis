import csv
import dataclasses
import json
from collections import defaultdict
from dataclasses import dataclass

import requests

from dataengweekly_analysis.data_path import get_path, get_csv_path

access_token = '3320e3ad33493df8538e9c295522bb714e50e32f'


def headers():
    return {
        'Authorization': 'Bearer {key}'.format(key=access_token)
    }


@dataclass
class ClickSummary:
    bitly_id: str
    short_link: str
    long_link: str
    title: str
    click_count: int
    edition: str


def get_org_guid() -> str:
    response = requests.get(
        "https://api-ssl.bitly.com/v4/organizations",
        headers=headers())
    json_data = json.loads(response.text)
    org_data = json_data['organizations'][0]
    return org_data['guid']


def get_group_guid(org_guid: str) -> str:
    response = requests.get(
        f"https://api-ssl.bitly.com/v4/groups?organization_guid={org_guid}",
        headers=headers())
    json_data = json.loads(response.text)
    group_guid = json_data['groups'][0]
    return group_guid['guid']


def get_clicks_summary_by_id(bitly_id: str) -> int:
    response = requests.get(
        f"https://api-ssl.bitly.com/v4/bitlinks/{bitly_id}/clicks/summary?unit=day&units=30&size=30",
        headers=headers())
    json_data = json.loads(response.text)
    return json_data['total_clicks']


def links_by_group(group_guid: str, link_dict: defaultdict, url: str):
    response = requests.get(url, headers=headers())
    json_data = json.loads(response.text)
    bit_links = json_data['links']
    for bit_link in bit_links:
        bitly_id = bit_link['id']
        edition = bit_link['tags'][0]
        click_count = get_clicks_summary_by_id(bitly_id)
        click_summary = ClickSummary(bitly_id=bitly_id, short_link=bit_link['link'], long_link=bit_link['long_url'],
                                     click_count=click_count,
                                     title=bit_link['title'], edition=edition)
        link_dict[edition].append(click_summary)

    next_page = json_data['pagination']['next']
    if next_page:
        links_by_group(group_guid, link_dict, next_page)


def get_country_metrics(bitly_id: str, country_metrics: dict):
    response = requests.get(
        f"https://api-ssl.bitly.com/v4/bitlinks/{bitly_id}/countries?unit=day&units=30&size=30",
        headers=headers())
    json_data = json.loads(response.text)
    metrics = json_data['metrics']
    total_count = 0
    click_dict = {}
    country_map = get_country_map()
    for metric in metrics:
        country = metric['value']
        click_count = int(metric['clicks'])
        total_count += click_count
        key = country_map.get(country, 'Others')
        click_dict[key] = click_dict.get(key, 0) + click_count
    for k, v in click_dict.items():
        country_metrics[k] = str(round((int(v) / int(total_count)) * 100, 2))
    return country_map


def get_country_map() -> dict:
    country_map = {}
    with open(get_csv_path('country')) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                country_map[row[1].replace('"', '').strip()] = row[0].replace('"', '').strip()
                line_count += 1
    return country_map


def write_country_metrics(link_list: defaultdict):
    country_metrics = {}
    for key, value in link_list.items():
        for click_summary in value:
            get_country_metrics(click_summary.bitly_id, country_metrics)

    json_object = json.dumps(country_metrics, indent=4)
    with open(get_path('country_metrics'), "w") as metrics:
        metrics.write(json_object)


def write_top3_links(link_list: defaultdict):
    link_to_write = []
    for key, value in link_list.items():
        sorted_link = sorted(value, key=lambda x: x.click_count, reverse=True)[:3]
        for link in sorted_link:
            link_to_write.append(dataclasses.asdict(link))

    json_object = json.dumps(link_to_write, indent=4)
    with open(get_path('top3_links'), "w") as metrics:
        metrics.write(json_object)


def main():
    org_guid = get_org_guid()
    group_guid = get_group_guid(org_guid)
    link_dict = defaultdict(list)
    url = f"https://api-ssl.bitly.com/v4/groups/{group_guid}/bitlinks?size=25&page=1"
    links_by_group(group_guid, link_dict, url)
    write_country_metrics(link_dict)
    write_top3_links(link_dict)


if __name__ == '__main__':
    main()
