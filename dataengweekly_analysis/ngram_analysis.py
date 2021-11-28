import asyncio
import json
import os
from urllib.parse import urlparse, urljoin

import requests
from aiostream import stream
from bs4 import BeautifulSoup
import yake

from dataengweekly_analysis.extractor import Extractor, ContentExtractor, URLExtractor


def is_valid(url: str) -> bool:
    """
    Check if the url is valid or not
    :param url:
    :return:
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_links(url):
    """
    Extract all the a href links from the given url.
    :param url:
    :return:
    """
    urls = set()
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url).content, "html.parser")

    for anchor_tag in soup.find_all('a'):
        href = anchor_tag.attrs.get('href')
        if href == "" or href is None:
            continue

        href = urljoin(url, href)
        parsed_href = urlparse(href)
        href = parsed_href.scheme + '://' + parsed_href.netloc + parsed_href.path

        if domain_name in href:
            continue

        if 'signup' in href:
            continue

        if 'javascript' in href:
            continue

        if 'ccpa' in href:
            continue

        if 'ananth' in href:
            continue
        if 'https://substack.com' == href:
            continue

        if 'substack' in href and 'support' in href:
            continue

        if 'twitter' in href and 'status' in href:
            continue

        if 'montecarlodata' in href or 'rudderstack' in href:
            continue

        if 'youtube' in href:
            continue

        urls.add(href)
    return urls


def weekly_links() -> set:
    """
    The pattern for weekly links are very predictable. Let's create a list of weekly links.
    No complex scrapping required here.
    :return:
    """
    editions = set()
    for edition in range(24, 66):
        url = 'https://www.dataengineeringweekly.com/p/data-engineering-weekly-{edition}'.format(edition=edition)
        if is_valid(url):
            editions.add(url)
        else:
            print('Found invalid url {url}'.format(url=url))
    return editions


def extract_keywords(url: str, ngram_size: int, num_of_keywords: int, extractor: Extractor):
    """
    Extract the keywords from the content
    :param url: url to extract the keywords
    :param ngram_size: size of the ngram. range 1 to 3
    :param num_of_keywords: Total number of keywords to return
    :param extractor: The extraction strategy to use. The current strategies are URLExtractor & ContentExtractor
    :return: list of keywords extracted
    """
    language = "en"
    max_ngram_size = ngram_size
    deduplication_threshold = 0.9
    keyword_dict = dict[str, int]()
    try:
        custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold,
                                                    top=num_of_keywords, features=None)
        keywords = custom_kw_extractor.extract_keywords(extractor.extract(url))

        for keyword in keywords:
            if keyword[0] in keyword_dict:
                keyword_dict[keyword[0].lower()] += 1
            else:
                keyword_dict[keyword[0].lower()] = 1
    except Exception as e:
        print(f'Error in parsing {url}')

    return keyword_dict


async def run_ngram_analysis(urls: list[str], ngram_size: int, num_of_keywords: int, extractor: Extractor):
    """
    Run the N-Gram analysis and format the keywords in a dictionary format
    :param urls: url to extract the keywords
    :param ngram_size: size of the ngram. range 1 to 3
    :param num_of_keywords: Total number of keywords to return
    :param extractor: The extraction strategy to use. The current strategies are URLExtractor & ContentExtractor
    :return: None
    """
    url_stream = stream.iterate(urls)
    extract = stream.map(url_stream, (lambda url: extract_keywords(url, ngram_size, num_of_keywords, extractor)),
                         ordered=False, task_limit=20)
    keyword_dict = dict[str, int]()
    async with extract.stream() as streamer:
        async for keywords in streamer:
            print(keywords)
            for key in keywords:
                if key in keyword_dict:
                    keyword_dict[key] += keywords[key]
                else:
                    keyword_dict[key] = keywords[key]

    await extract

    result: dict[str, int] = dict()

    for key in keyword_dict:
        if keyword_dict[key] >= 3:
            result[key] = keyword_dict[key]

    # Write the output into a JSON file
    filename = os.path.dirname(os.path.abspath(__file__)) + '/data/' + extractor.get_name() + str(ngram_size) + ".json"
    with open(filename, 'w') as file:
        file.write(json.dumps(result))
        file.flush()
        file.close()


async def execute(urls: list[str]):
    """
    Execute the N-Gram analysis for the list of urls
    :param urls: list of urls
    :return: None
    """
    analysis = [run_ngram_analysis(urls=urls, ngram_size=3, num_of_keywords=5, extractor=ContentExtractor()),
                run_ngram_analysis(urls=urls, ngram_size=2, num_of_keywords=5, extractor=ContentExtractor()),
                run_ngram_analysis(urls=urls, ngram_size=1, num_of_keywords=5, extractor=ContentExtractor()),
                run_ngram_analysis(urls=urls, ngram_size=2, num_of_keywords=2, extractor=URLExtractor()),
                run_ngram_analysis(urls=urls, ngram_size=1, num_of_keywords=2, extractor=URLExtractor())]
    await asyncio.gather(*analysis, return_exceptions=True)


def domain_names(urls: list[str]):
    """
    Run domain name analysis on a given url
    :param urls: list of urls
    :return: None
    """
    result: dict[str, int] = dict()
    for url in urls:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if "medium" in domain:  # If the blog published in medium, get the next first part of the URL path
            domain = parsed_url.path.strip('/').split('/')[0].replace('-', ' ')

        domain = domain.lower()
        if domain in result:
            result[domain] += 1
        else:
            result[domain] = 1

    filename = os.path.dirname(os.path.abspath(__file__)) + '/data/domain.json'
    with open(filename, 'w') as file:
        file.write(json.dumps(result))
        file.flush()
        file.close()


if __name__ == '__main__':

    # step 1: Get all the data engineering weekly links
    weekly_links = weekly_links()

    # step 2: For each data engineering weekly, extract all the links shared in each edition.
    article_links = list[str]()
    for weekly_link in weekly_links:
        for article_link in get_all_links(weekly_link):
            article_links.append(article_link)

    # step 3: Run domain name analysis
    domain_names(article_links)

    # step 4: Run N-Gram analysis
    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(article_links))
    loop.close()
