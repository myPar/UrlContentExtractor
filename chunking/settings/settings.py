from pydantic import BaseModel
import pydantic_core
from pydantic import BaseModel


class PipelineSettings(BaseModel):
    use_pipeline: bool
    broker_host: str
    broker_port: int


class Settings(BaseModel):
    model_path: str
    file_path: str
    output: str
    dir_path: str
    chunk_size: int
    pipeline_settings: PipelineSettings
    lang: str
    delimiter: str


with open('settings/settings.json', encoding='utf-8') as f:
    json_data = f.read()

chunker_settings = Settings.model_validate(pydantic_core.from_json(json_data))