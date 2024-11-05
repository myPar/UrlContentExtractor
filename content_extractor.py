from tqdm import tqdm
from bs4 import BeautifulSoup
import os
import httpx
import re
import sys


class UrlContentExtractor:
    def __init__(self, urls, save: bool=True, save_dir: str='data/'):
        self._urls = urls
        self._save = save
        self._save_dir = save_dir
        self._min_characters_count = 300

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
                resp = httpx.get(url)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')
                extracted_text = re.sub(r'[\n\r\t]+', '\n', soup.get_text()).replace('\u00A0', ' ')

                if len(extracted_text.replace(' ','')) < self._min_characters_count:
                    continue
                with open(os.path.join(self._save_dir, re.sub(f'https?://','', url).replace('/','').replace('.','').replace(':', '_')) + ".txt", 'w', encoding='utf-8') as f:
                    f.write(extracted_text)
            except httpx.HTTPStatusError as e:
                if log:
                    print(f"error response for url='{url}': status={e.response.status_code}; {repr(e)}", file=sys.stderr)
            except httpx.RequestError as e:
                if log:
                    print(f"request error for url='{url}': {str(e)}", file=sys.stderr)
            pbar.update(1)
