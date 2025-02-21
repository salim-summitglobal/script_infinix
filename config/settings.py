from functools import lru_cache
from pydantic_settings import BaseSettings



class Settings(BaseSettings):

    PROJECT_NAME: str


    # AWS
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    AWS_BUCKET_NAME: str



    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()