from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    APP_ENV: str = "development"
    DEBUG: bool = True
    CHUNK_SIZE: int = 5000

    class Config:
        env_file = ".env"


settings = Settings()