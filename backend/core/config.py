from typing import List, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CLOUDOS_",
        extra="ignore",
    )

    APP_NAME: str = "CloudOS-RL"
    DEBUG: bool = False
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:3001",
    ]

    KAFKA_BOOTSTRAP: str = "localhost:9092"
    AWS_REGION: str = "us-east-1"
    aws_profile: Optional[str] = None

    MODEL_PATH: str = "models/best/best_model"
    VECNORM_PATH: str = "models/vec_normalize.pkl"
    CONFIG_PATH: str = "config/settings.yaml"
    LOG_LEVEL: str = "INFO"