import copy
import re
from urllib.parse import unquote, urljoin, urlparse
from bs4 import BeautifulSoup
from typing import List
import httpx
from status import redirects
import sys


text_tags = {'div', 'dl', 'dt', 'li', 'menu', 'ol', 'p',
             'ul', 'ol', 'b', 'span', 'hgroup', 'h1', 'h2',
             'h3', 'h4', 'h5', 'h6', 'title'}

table_tags = {'caption', 'col', 'colgroup', 'table', 'tbody', 'td', 'tfoot', 'th', 'thead', 'tr'}

inline_text_semantic_tags = {'abbr', 'b', 'bdi', 'cite', 'code', 'data', 'dfn', 'em',
                             'i', 'kbd', 'mark', 'q', 'blockquote', 'samp', 'small',
                             'span', 'strong', 'sub', 'time', 'u'}

# old sites still using it
deprecated_text_tags = {'big', 'center', 'font'}

all_tags = text_tags.union(table_tags).union(inline_text_semantic_tags).union(deprecated_text_tags)


def create_url_file_name(url: str):
    cleaned_url = re.sub(r'^https?://', '', url)
    cleaned_url = re.sub(r'^www\.', '', cleaned_url)
    return cleaned_url.replace('/', '') \
                        .replace('.', '') \
                        .replace(':', '_') \
                        .replace('?', '') + ".txt"


class UrlMetaData:
    def __init__(self, url:str):
        self.url = url
        self.title = None
        self.h1 = None
        self.h2 = None
        self.h3 = None
        self.h4 = None
        self.h5 = None
        self.h6 = None
        self.input_url_name = None
        self.format = 'html'

    def get_dict(self):
        result = dict()
        result['title'] = self.title
        result['h1'] = self.h1
        result['h2'] = self.h2
        result['h3'] = self.h3
        result['h4'] = self.h4
        result['h5'] = self.h5
        result['h6'] = self.h6
        result['url'] = self.url
        result['input_url_name'] = self.input_url_name

        return result    


def try_join_url(parent: str, child: str):
    child_scheme = urlparse(child).scheme.strip()

    if child_scheme == '':
        return urljoin(parent, child)
    return child


def filter_non_http(urls: List[str]):
    def is_http(url: str):
        scheme = urlparse(url).scheme.strip().lower()
        return scheme == 'http' or scheme == 'https'

    result = [url for url in urls if is_http(url)]
    return result


def drop_html_artifacts(text: str):
    text = re.sub(r'\xa0', r' ', text)
    text = re.sub(r'\t', r' ', text)
    text = re.sub(r'(\s){2,}', r'\1', text)
    return text


class HtmlScrapper:
    def __init__(self, base_url: str, input_url_name: str=None, log: bool=True, only_urls:bool=False):
        self.url = base_url
        self.http_client = httpx.AsyncClient(timeout=30)
        self.log = log
        self.init = False
        self.input_url_name = input_url_name
        self.only_urls = only_urls

    # loads html for self.url and init body
    async def init_scrapper(self):
        exception = None
        # extract html body in constructor:
        try:
            response = await self.http_client.get(self.url)
            if response.status_code in redirects:
                redirected_url = response.headers['Location']
                response = await self.http_client.get(redirected_url)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            exception_msg = f"error response for url='{self.url}': status={e.response.status_code}; {repr(e)}"
            exception = Exception(exception_msg)

        except httpx.RequestError as e:
            exception_msg = f"request error for url='{self.url}': {str(e)}"
            exception = Exception(exception_msg)

        except Exception as e:
            exception_msg = f"another error for url={self.url}': {str(e)}"
            exception = Exception(exception_msg)
        if exception is not None:
            if self.log:
                print(exception_msg, file=sys.stderr, flush=True)
            raise exception
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            self.body = soup.find('body')
            if self.body is None:
                raise Exception('empty body in html document')
        except Exception as e:
            exception_msg = f"can't parse text from url={self.url}: {str(e)}"
            if self.log:
                print(exception_msg, file=sys.stderr, flush=True)
            raise Exception(exception_msg)
        self.init = True

    # returns child urls with it's names in document
    def extract_child_urls(self) -> tuple[List[str], dict[str: str]]:
        def get_url_data(url_tag, base_url):
            url_string = url_tag.get('href').strip()
            text = url_tag.get_text().strip()
            result_url = unquote(try_join_url(base_url, url_string))
            return result_url, text

        def drop_empty_links(links):
            return [link for link in links if link.get('href') is not None]

        if not self.init:
            raise Exception('scrapper is not initialized')

        urls = self.body.find_all('a')
        urls = drop_empty_links(urls)
        child_urls_data = [get_url_data(url_tag, self.url) for url_tag in urls]
        child_urls = [data[0] for data in child_urls_data]
        names = [data[1] for data in child_urls_data]
        names_dict = {url: name for url, name in zip(child_urls, names)}

        # filter not http refs and drop duplicates:
        child_urls = filter_non_http(list(set(child_urls)))

        return child_urls, names_dict

    def extract_text(self):
        if not self.init:
            raise Exception('scrapper is not initialized')
        if self.only_urls:  # only_urls mode doesn't extract text
            return ""
        body = copy.deepcopy(self.body)
        tags_to_remain = set()

        for tag in body.find_all('a'):
            if 'p' not in [par.name for par in tag.parents]:
                tag.replace_with('')
            else:
                tags_to_remain.add(tag)

        for tag in body.find_all():
            if tag.name is not None and tag.name.lower() not in all_tags and tag not in tags_to_remain:
                tag.replace_with('')

        return drop_html_artifacts(body.get_text())

    def get_meta(self) -> dict:
        def get_text(bs_tag):
            if bs_tag is not None:
                return bs_tag.get_text().strip()
            return ""

        if not self.init:
            raise Exception('scrapper is not initialized')
        result = dict()
        title = get_text(self.body.find('title'))
        h1 = get_text(self.body.find('h1'))
        h2 = get_text(self.body.find('h2'))
        h3 = get_text(self.body.find('h3'))
        h4 = get_text(self.body.find('h4'))
        h5 = get_text(self.body.find('h5'))
        h6 = get_text(self.body.find('h6'))
        url = self.url
        result['title'] = title
        result['h1'] = h1
        result['h2'] = h2
        result['h3'] = h3
        result['h4'] = h4
        result['h5'] = h5
        result['h6'] = h6
        result['url'] = url
        result['input_url_name'] = self.input_url_name

        return result
