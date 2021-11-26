from abc import abstractmethod

import requests
from bs4 import BeautifulSoup


class Extractor:

    @abstractmethod
    def extract(self, url: str) -> str:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass


class ContentExtractor(Extractor):

    def extract(self, url: str) -> str:
        response = requests.get(url)
        html_page = response.content
        soup = BeautifulSoup(html_page, 'html.parser')
        text = soup.find_all(text=True)
        output = ''
        deny_list = [
            '[document]',
            'noscript',
            'header',
            'html',
            'meta',
            'head',
            'input',
            'script',
            'style',
            'link',
            'button',
            'img',
        ]

        for t in text:
            if t.parent.name not in deny_list:
                output += '{} '.format(t)
        return output

    def get_name(self) -> str:
        return 'content_extractor'


class URLExtractor(Extractor):

    def extract(self, url: str) -> str:
        splitter = url.strip('/').split('/')
        return splitter[len(splitter) - 1].replace('-', ' ')

    def get_name(self) -> str:
        return 'url_extractor'
