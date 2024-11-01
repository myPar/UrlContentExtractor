import httpx
import re
from typing import List
import sys


class UrlExtractor:
    def __init__(self, max_depth:int=2, reject_http:bool=False, ignored_domens: List[str]=None, max_urls:int=None):
        self._max_depth = max_depth
        self._reject_http = reject_http
        self._ignored_domens = ignored_domens if ignored_domens is not None else []
        self._max_urls = max_urls if max_urls is not None and max_urls > 0 else None
        self._urls_count = 0
        self.log = False

    def set_max_urls(self, max_urls: int):
        assert max_urls >= 1
        self._max_urls = max_urls

    def _extract_child_refs(self, url):
        result = []
        try:
            response = httpx.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if self.log:
                print(f"error response for url='{url}': status={e.response.status_code}; {repr(e)}", file=sys.stderr)
            return []
        except httpx.RequestError as e:
            if self.log:
                print(f"request error for url='{url}': {str(e)}", file=sys.stderr)
            return []
        urls = re.findall(r'<.*href=".+">', response.text)

        for url in urls:
            url_text = re.search(r'href="(.*?)"', url)[0].split('=')[-1].replace('"', '').strip()
            result.append(url_text)
        return result

    def _is_rejected(self, url: str):
        return self._reject_http and re.match(r'http://.+',url)

    def _supplement_child_url(self, base_url: str, url):
        if re.search(r'https?://.+', url) is None:
            return base_url + url if base_url[-1] == '/' else base_url + "/" + url
        return url

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
        def has_domen(item):
            for domen in self._ignored_domens:
                if domen in item:
                    return True
            return False
        if len(self._ignored_domens) > 0:
            return set(item for item in result_urls if not has_domen(item))
        return result_urls

    def _filter_similar_urls(self, urls):
        def remove_ident(url):
            idx = url.find('#item')
            res = url
            if idx != -1:
                res = url[:idx]
            if res[-1] != '/':
                return res + "/"
            return res
        return {remove_ident(url) for url in urls}

    def extract(self, base_url: str, log: bool=False)->set[str]:
        self.log = log
        self._urls_count = 0
        urls_queue = []
        base_url = self._supplement_base_url(base_url)  # add https if necessary
        urls_queue.append((base_url, 1))
        result = set()

        while len(urls_queue) > 0:
            cur_url_data = urls_queue.pop(0)
            cur_url, cur_depth = cur_url_data
            if log:
                self._log_item(cur_url_data)
            if cur_depth < self._max_depth and not self._is_rejected(cur_url) and not self._enough_urls():
                child_urls = self._extract_child_refs(cur_url)

                for i in range(len(child_urls)):
                    if self._enough_urls():
                        break
                    child_urls[i] = self._supplement_child_url(base_url, child_urls[i])

                    if child_urls[i] not in result:
                        result.add(child_urls[i])
                        urls_queue.append((child_urls[i], cur_depth + 1))
                        self._urls_count += 1

        return self._filter_similar_urls(self._filter_domens(result))
