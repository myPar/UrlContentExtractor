from tqdm import tqdm
from bs4 import BeautifulSoup
import os
import httpx
import re
import sys
from http import HTTPStatus
from status import redirects
from typing import List
import json


class UrlContentExtractor:
    def __init__(self, urls, save: bool=True, save_dir: str='data/', exclude_files=None):
        self._urls = urls
        self._save = save
        self._save_dir = save_dir
        self._min_characters_count = 300
        self._exclude_files = None if exclude_files is None else set(exclude_files)
        self._url_dict = dict()

    def _create_file_name(self, url: str):
        return re.sub(f'https?://', '', url).replace('/', '').replace('.', '').replace(':', '_') + ".txt"

    def _filter_similar_urls(self):
        def remove_ident(url):
            idx = url.find('#')
            if idx != -1:
                return url[:idx]
            return url

        return {remove_ident(url) for url in self._urls}

    def extract_content(self, log: bool = False):
        if not os.path.isdir(self._save_dir):
            os.mkdir(self._save_dir)

        urls_to_extract = self._filter_similar_urls()
        pbar = tqdm(total=len(urls_to_extract))

        for url in urls_to_extract:
            try:
                res_file_name = self._create_file_name(url)
                # skip files already exists in result dir:
                if self._exclude_files is not None and res_file_name in self._exclude_files:
                    if log:
                        print(f"file for url={url} is already exists in exclude dir")
                    continue
                resp = httpx.get(url)
                if resp.status_code in redirects:
                    redirected_url = resp.headers['Location']
                    resp = httpx.get(redirected_url)
                resp.raise_for_status()
                try:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                except Exception as e:
                    if log:
                        print(f"can't parse text from url={url}: {str(e)}")
                    continue
                extracted_text = re.sub(r'[\n\r\t]+', '\n', soup.get_text()).replace('\u00A0', ' ')

                if len(extracted_text.replace(' ', '')) < self._min_characters_count:
                    continue
                with open(os.path.join(self._save_dir, res_file_name), 'w', encoding='utf-8') as f:
                    f.write(extracted_text)
                    self._url_dict[res_file_name] = url
            except httpx.HTTPStatusError as e:
                if log:
                    print(f"error response for url='{url}': status={e.response.status_code}; {repr(e)}",
                          file=sys.stderr)
            except httpx.RequestError as e:
                if log:
                    print(f"request error for url='{url}': {repr(e)}", file=sys.stderr)
            except Exception as e:
                if log:
                    print(f"another error occured: {repr(e)}")
            pbar.update(1)

    def save_url_dict(self):
        json_data = json.dumps(self._url_dict)

        with open(os.path.join(self._save_dir, 'urls_dict.json'), 'w') as f:
            f.write(json_data)

