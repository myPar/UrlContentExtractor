from pydantic import BaseModel
import pydantic_core
import json
from typing import List

class LaunchSettings(BaseModel):
    base_url: str
    depth: int
    max_urls: int
    output: str
    log: bool

class UrlPolicy(BaseModel):
    only_urls: bool
    add_urls: bool
    urls_file_name: str
    update_old_urls: bool


class PipelineSettings(BaseModel):
    use_pipeline: bool
    broker_host: str
    broker_port: int


class Settings(BaseModel):
    urls_policy: UrlPolicy
    reject_http: bool
    load_pdf: bool
    medias: List[str]
    ignored_domens: List[str]
    pipeline_settings: PipelineSettings
    min_content_size: int
    launch: LaunchSettings


with open('settings/settings.json', encoding='utf-8') as f:
    json_data = f.read()


crawler_settings = Settings.model_validate(pydantic_core.from_json(json_data))
