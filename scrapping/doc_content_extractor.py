import asyncio
from pypdf import PdfReader
import httpx
import io
import re
import os
from html_tools import create_url_file_name


class DocContentExtractorException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class DocContentExtractor():
    def __init__(self, save_dir: str = None, min_content_size: int=50):
        self.load_timeout = 30
        self.http_client = None
        self.save_dir = "data" if save_dir is None else save_dir
        self.min_content_size = min_content_size

    async def _init_client(self):
        if self.http_client is None:
            self.http_client = httpx.AsyncClient()

    async def __aenter__(self):
        await self._init_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.http_client is not None:
            await self.http_client.aclose()

    def _save_pdf_content(self, reader: PdfReader, url: str):
        content = ""

        for page in reader.pages:
            try:
                content += page.extract_text() + "\n"
            except Exception as e:
                continue
        f_name = create_url_file_name(url)

        if len(content.strip()) >= self.min_content_size:
            with open(os.path.join(self.save_dir, f_name), 'w', encoding='utf-8') as f:
                f.write(content.strip())

    async def extract_content(self, url, format: str):
        if self.http_client is None:
            raise DocContentExtractorException('not initialized')
        try:
            resp = await self.http_client.get(url, timeout=self.load_timeout)
            if format == 'pdf':
                pdf_reader = PdfReader(io.BytesIO(resp.content))
                asyncio.create_task(asyncio.to_thread(self._save_pdf_content, pdf_reader, url))
            else:
                raise DocContentExtractorException(f'unsupported format: {format}')
        except Exception as e:
            raise DocContentExtractorException(str(e))
