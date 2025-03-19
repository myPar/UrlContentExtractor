import copy
import json
import httpx
import re
from typing import List
import sys
import asyncio
from itertools import chain
import os
import aiofiles
from doc_content_extractor import DocContentExtractor, DocContentExtractorException
from settings.settings import Settings
from html_tools import HtmlScrapper, UrlMetaData
from collections import deque
from urllib.parse import unquote
from broker import BrokerAdapter
from html_tools import create_url_file_name
import json

doc_formats = {'pdf'}


class UrlHandleTask:
    def __init__(self, url, depth:int=0, name:str=None):
        self.url = url
        self.depth = depth
        self.url_name = name


class BrokerTask:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_json(self) -> str:
        result = dict()
        result['file_path'] = self.file_path

        return json.dumps(result)


def remove_ident(url):
    idx = url.find('#')
    res = url
    if idx != -1:
        res = url[:idx]
    return res


def remove_ident_urls(url_name_dict: dict):
    url_name_dict = copy.deepcopy(url_name_dict)
    urls = list(url_name_dict.keys())
    
    for url in urls:
        url_cleared = remove_ident(url)
        if url_cleared != url:
            popped_item = url_name_dict.pop(url)
            url_name_dict[url_cleared] = popped_item
    return url_name_dict


class UrlExtractor:
    def __init__(self, settings: Settings, max_depth:int=2, ignored_domens: List[str]=None,
                 required_domens: List[str]=None, max_urls:int=None, exclude_files=None,
                 save_dir: str=None, use_pipeline:bool=False):
        self._max_depth = max_depth
        self._ignored_domens = ignored_domens if ignored_domens is not None else []
        self._required_domens = required_domens if required_domens is not None else []
        self._max_urls = max_urls if max_urls is not None and max_urls > 0 else None
        self.processed_urls_count = 0   # urls which were processed and content extracted
        self.log = False
        self._exclude_files = None if exclude_files is None else set(exclude_files)
        self.http_client = httpx.AsyncClient(timeout=30)

        self._save_dir = save_dir if save_dir is not None else "data/"
        if not os.path.isdir(self._save_dir):
            os.mkdir(self._save_dir)
        self._step = 25
        self.settings = settings

        # init broker adapter:
        pipeline_settings = self.settings.pipeline_settings
        self.broker_adapter = BrokerAdapter(pipeline_settings.broker_host, pipeline_settings.broker_port, use_pipeline)
        self.broker_adapter.init_adapter()

        self.urls_cache = set()
        self.meta_dict = dict() # meta info about handled urls

        self.msg_cache = set()

    def set_max_urls(self, max_urls: int):
        assert max_urls >= 1
        self._max_urls = max_urls

    def _is_media(self, url: str) -> bool:
        format = url.split(".")[-1]
        return format.lower() in self.settings.medias

    def _is_rejected(self, url: str):
        return self.settings.reject_http and re.match(r'http://.+', url)

    def _supplement_base_url(self, base_url: str):
        if re.search(r'https?://.+', base_url) is None:
            return 'https://' + base_url
        return base_url

    def enough_urls(self):
        return self._max_urls is not None and self.processed_urls_count >= self._max_urls

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
            return list(set(item for item in result_urls if not has_ignored_domen(item) and has_required_domen(item)))
        return result_urls

    def _filter_similar_urls(self, urls):
        return list({remove_ident(url) for url in urls})

    def _filter_exclude_urls(self, urls: List[str]):
        return [url for url in urls if create_url_file_name(url) not in self._exclude_files]
    
    def _filter_cached_urls(self, urls: List[str]):
        return [url for url in urls if url not in self.urls_cache]

    # filter urls:
    # 1. which has no any required domen
    # 2. which has any ignored domen
    # 3. url which file is excluded
    # 4. url which are already processed or in queue
    def remove_bad_urls(self, urls: List[str]):
        return self._filter_cached_urls(self._filter_exclude_urls(self._filter_similar_urls(self._filter_domens(urls))))

    # several courotine can contain same UrlHandleTask so we need to drop duplicated
    def drop_duplicated_tasks(self, tasks: List[UrlHandleTask]):
        result = []
        urls_set = set()

        for task in tasks:
            if task.url not in urls_set:
                urls_set.add(task.url)
                result.append(task)
        return result

    async def save_extracted_text(self, extracted_text: str, url):
        # write extracted text to the result file
        res_file_name = create_url_file_name(url)
        try:
            if len(extracted_text.strip()) >= self.settings.min_content_size:
                async with aiofiles.open(os.path.join(self._save_dir, res_file_name), 'w', encoding='utf-8') as f:
                    await f.write(extracted_text.strip())
        except Exception as e:
            raise e

    # extracts text by url and returns child refs tasks: UrlHandleTask
    async def url_handle_routine(self, task: UrlHandleTask) -> List[UrlHandleTask]:
        if task.depth > self._max_depth or self._is_rejected(task.url) or self.enough_urls():
            return []
        format = task.url.split('.')[-1]
        out_file_name = create_url_file_name(task.url)
        broker_task = BrokerTask(os.path.join(self._save_dir, out_file_name))    # create broker task

        # extract doc file if necessary:
        if format in doc_formats:   # supported formats: pdf
            # documents extracting (pdf):
            if self.settings.load_pdf:
                try:
                    # extract document content if only_urls is disabled:
                    if not self.settings.urls_policy.only_urls:
                        async with DocContentExtractor(save_dir=self._save_dir) as extractor:
                            await extractor.extract_content(task.url, format)
                            if self.log:
                                print(f'[{task.depth}] {task.url} is processed')
                    meta = UrlMetaData(task.url)
                    meta.format = format
                    self.meta_dict[task.url] = meta.get_dict()

                    # push task to broker if file was successfully saved:
                    if os.path.isfile(os.path.join(self._save_dir, out_file_name)) and out_file_name not in self.msg_cache:
                        self.broker_adapter.push_message(message=broker_task.get_json())
                        self.msg_cache.add(out_file_name)
                except DocContentExtractorException as e:
                    if self.log:
                        print(f"invalid doc for text extraction from url={task.url}: {str(e)}", file=sys.stderr)
            return []   # no child urls for document
        if self._is_media(task.url): # ignore medias (zip, png, jpg ans s.o.)
            return []
        # scrap html (extract content, child refs and ref's names):
        scrapper = HtmlScrapper(task.url, task.url_name, log=self.log, only_urls=self.settings.urls_policy.only_urls)
        try:
            await scrapper.init_scrapper()
            extracted_text = scrapper.extract_text()
            urls, urls_names_dict = scrapper.extract_child_urls()
            meta = scrapper.get_meta()
            self.meta_dict[task.url] = meta # add meta info for handled url
        except Exception as e:
            if self.log:
                print("html scrapping error: " + str(e))
                return []
        # save extracted text to file:
        try:
            if not self.settings.urls_policy.only_urls:
                await self.save_extracted_text(extracted_text, task.url)
                # push task to broker if file was saved:
                if os.path.isfile(os.path.join(self._save_dir, out_file_name)) and out_file_name not in self.msg_cache:
                    self.broker_adapter.push_message(message=broker_task.get_json())
                    self.msg_cache.add(out_file_name)
            self.processed_urls_count += 1   # url was successfully processed
            if self.log:
                print(f'[{task.depth}] {task.url} is processed')
        except Exception as e:
            if self.log:
                print(f"I/O exception, while saving {task.url} content': {str(e)}", file=sys.stderr, flush=True)
        # filter urls and construct the result
        child_urls = self.remove_bad_urls(urls)
        urls_names_dict = remove_ident_urls(urls_names_dict)
        result = []

        for child_url in child_urls:
            result.append(UrlHandleTask(child_url, task.depth + 1, urls_names_dict[child_url]))

        return result

    def filter_existing_urls_data(self, urls_data_list: List[tuple[str, int]], result_set):
        return [(url, depth) for url, depth in urls_data_list if url not in result_set]

    def get_urls(self, urls_data_list: List[tuple[str, int]]):
        return {item[0] for item in urls_data_list}

    async def extract(self, base_url: str, log: bool = False):
        self.urls_cache = set()
        self.meta_dict = dict() # meta info about handled urls

        self.log = log
        self._urls_count = 0
        base_url = unquote(base_url)
        base_url = self._supplement_base_url(base_url)  # Add https if necessary

        urls_queue = deque([UrlHandleTask(base_url, 1, "")])  # Queue with (URL, depth)
        self.urls_cache.add(base_url)

        while urls_queue and not self.enough_urls():
            # Calculate how many URLs to process in this step
            remaining_count = self._max_urls - self._urls_count
            batch_size = min(remaining_count, len(urls_queue), self._step)

            # Get the next batch of URLs and depths to process
            cur_urls_tasks = [urls_queue.popleft() for _ in range(batch_size)]   # get tasks

            # Process URLs concurrently
            coros = [self.url_handle_routine(task) for task in cur_urls_tasks]
            coros_results = await asyncio.gather(*coros)

            # Flatten results
            new_urls_tasks = list(chain(*coros_results))
            # drop duplicated tasks (with the same url):
            new_urls_tasks = self.drop_duplicated_tasks(new_urls_tasks)

            # Add new URLs to the queue and url's cache
            urls_queue += new_urls_tasks
            self.urls_cache.update(task.url for task in new_urls_tasks)

    def save_meta_dict(self):
        file_name = self.settings.urls_policy.urls_file_name
        file_name = os.path.join(self._save_dir, file_name)
        existing_dict = dict()
        # get old records if exists
        if self.settings.urls_policy.add_urls:
            if os.path.isfile(file_name):
                with open(file_name, 'r', encoding='utf-8') as f:
                    content = f.read()
                    existing_dict = json.loads(content)
        if self.settings.urls_policy.update_old_urls:
            existing_dict.update(self.meta_dict)    # add new records with updating existing records
            result = existing_dict
        else:
            self.meta_dict.update(existing_dict)    # add new records without updating old
            result = self.meta_dict

        json_data = json.dumps(result)
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(json_data)

