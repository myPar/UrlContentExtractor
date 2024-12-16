import json
from bs4 import BeautifulSoup
import httpx
import re
from typing import List
import sys
from urllib.parse import unquote, urljoin
from status import redirects
import asyncio
from itertools import chain
import os
import aiofiles


class UrlExtractor:
    def __init__(self, max_depth:int=2, reject_http:bool=False, ignored_domens: List[str]=None,
                 required_domens: List[str]=None, max_urls:int=None, exclude_files=None,
                 save_dir:str=None):
        self._max_depth = max_depth
        self._reject_http = reject_http
        self._ignored_domens = ignored_domens if ignored_domens is not None else []
        self._required_domens = required_domens if required_domens is not None else []
        self._max_urls = max_urls if max_urls is not None and max_urls > 0 else None
        self._urls_count = 0
        self.log = False
        self._exclude_files = None if exclude_files is None else set(exclude_files)
        self.http_client = httpx.AsyncClient(timeout=30)

        self._save_dir = save_dir if save_dir is not None else "data/"
        if not os.path.isdir(self._save_dir):
            os.mkdir(self._save_dir)
        self._url_dict = dict()
        self._step = 20
    def _create_file_name(self, url: str):
        return re.sub(f'https?://', '', url).replace('/', '').replace('.', '').replace(':', '_').replace('?', '') + ".txt"
    def set_max_urls(self, max_urls: int):
        assert max_urls >= 1
        self._max_urls = max_urls

    # main url routine - extracts child urls and text from corresponding html-doc
    # also saves extracted text to file
    async def _extract_child_refs(self, url):
        try:
            response = await self.http_client.get(url)
            if response.status_code in redirects:
                redirected_url = response.headers['Location']
                response = await self.http_client.get(redirected_url)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if self.log:
                print(f"error response for url='{url}': status={e.response.status_code}; {repr(e)}", file=sys.stderr, flush=True)
            return []
        except httpx.RequestError as e:
            if self.log:
                print(f"request error for url='{url}': {str(e)}", file=sys.stderr, flush=True)
            return []
        except Exception as e:
            if self.log:
                print(f"another error for url={url}': {str(e)}", file=sys.stderr, flush=True)
            return []
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            if self.log:
                print(f"can't parse text from url={url}: {str(e)}")
            return []
        urls = soup.find_all('a')
        extracted_text = re.sub(r'[\n\r\t]+', '\n', soup.get_text()).replace('\u00A0', ' ')
        result = [url.get('href') for url in urls]
        # write extracted text to the result file
        res_file_name = self._create_file_name(url)
        async with aiofiles.open(os.path.join(self._save_dir, res_file_name), 'w', encoding='utf-8') as f:
            await f.write(extracted_text)
        self._url_dict[res_file_name] = url

        return result

    def _is_rejected(self, url: str):
        return self._reject_http and re.match(r'http://.+',url)

    def _supplement_base_url(self, base_url: str):
        if re.search(r'https?://.+', base_url) is None:
            return 'https://' + base_url
        return base_url

    def _log_item(self, item):
            url, depth = item
            print('  '*(depth-1) +f"[{depth}] " + url)

    def _enough_urls(self):
        return self._max_urls is not None and self._urls_count >= self._max_urls

    def _filter_domens(self, result_urls):
        def has_ignored_domen(item):
            for domen in self._ignored_domens:  # at least one ignored domen in item
                if domen in item:
                    return True
            return False

        def has_required_domen(item):
            if len(self._required_domens) == 0:
                return True
            for domen in self._required_domens: # at least one required domen in item
                if domen in item:
                    return True
            return False
        if len(self._ignored_domens) > 0 or len(self._required_domens) > 0:
            return set(item for item in result_urls if not has_ignored_domen(item) and has_required_domen(item))
        return result_urls

    def _filter_similar_urls(self, urls):
        def remove_ident(url):
            idx = url.find('#')
            res = url
            if idx != -1:
                res = url[:idx]
            return res
        return list({remove_ident(url) for url in urls})

    def _filter_exclude_urls(self, urls: List[str]):
        return [url for url in urls if self._create_file_name(url) not in self._exclude_files]

    def _filter_garbage_urls(self, urls: List[str]):
        def has_gargage(url:str):
            return '@' in url or '?' in url
        return [url for url in urls if not has_gargage(url)]

    def remove_bad_urls(self, urls: List[str]):
        return self._filter_exclude_urls(self._filter_similar_urls(self._filter_domens(self._filter_garbage_urls(urls))))

    async def url_handle_routine(self, url: str, cur_depth: int, url_cache: set):
        if cur_depth > self._max_depth or self._is_rejected(url) or self._enough_urls():
            return {}
        child_urls = await self._extract_child_refs(url)
        # join base url with children and replace %xx escapes with their single-character equivalent
        child_urls = [unquote(urljoin(url, child_url)) for child_url in child_urls]
        child_urls = self.remove_bad_urls(child_urls)
        result = []

        for i in range(len(child_urls)):
            if self._enough_urls():
                break
            if child_urls[i] not in url_cache:
                url_cache.add(child_urls[i])
                url_item = (child_urls[i], cur_depth + 1)
                result.append(url_item)

        if self.log:
            self._log_item((url, cur_depth))
        self._urls_count += 1  # only current url with successful doc extraction is increment counter

        return result

    def filter_existing_urls_data(self, urls_data_list: List[tuple[str, int]], result_set):
        return [(url, depth) for url, depth in urls_data_list if url not in result_set]

    def get_urls(self, urls_data_list: List[tuple[str, int]]):
        return {item[0] for item in urls_data_list}

    async def extract(self, base_url: str, log: bool = False) -> set[str]:
        self.log = log
        self._urls_count = 0
        base_url = unquote(base_url)
        base_url = self._supplement_base_url(base_url)  # Add https if necessary

        urls_queue = [(base_url, 1)]  # Queue with (URL, depth)
        result = set()  # Final set of processed URLs
        all_urls = {base_url}  # Tracks all seen URLs (queued + processed)

        while urls_queue and not self._enough_urls():
            # Calculate how many URLs to process in this step
            remaining_count = self._max_urls - self._urls_count
            batch_size = min(remaining_count, len(urls_queue), self._step)

            # Get the next batch of URLs and depths to process
            cur_urls_data = urls_queue[:batch_size]
            urls_queue = urls_queue[batch_size:]  # Remove processed URLs from the queue

            # Unzip URLs and depths for batch processing
            urls, depths = zip(*cur_urls_data)

            # Process URLs concurrently
            coros = [self.url_handle_routine(url, depth, result) for url, depth in zip(urls, depths)]
            coros_results = await asyncio.gather(*coros)
            result.update(urls)
            all_urls.update(urls)

            # Flatten results and filter duplicates
            new_urls_data = list(chain(*coros_results))
            new_urls_data = self.filter_existing_urls_data(new_urls_data, all_urls)

            # Add new URLs to the queue and result set
            urls_queue += new_urls_data
            all_urls.update(url for url, _ in new_urls_data)

        # Ensure the base URL is included in the result
        result.add(base_url)
        return result

    def save_url_dict(self):
        json_data = json.dumps(self._url_dict)

        with open(os.path.join(self._save_dir, 'urls_dict.json'), 'w') as f:
            f.write(json_data)