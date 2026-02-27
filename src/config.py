from pydantic_settings import BaseSettings, SettingsConfigDict

class AppConfig(BaseSettings):
    port: int = 8000
    broker_url: str = "http://127.0.0.1:8000"
    queue_storage_filename: str = "queue"
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = AppConfig()
