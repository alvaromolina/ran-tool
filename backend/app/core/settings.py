import os
from typing import List
from dotenv import load_dotenv

# Load project-level .env
load_dotenv()


def _split_csv(value: str | None) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


class Settings:
    API_DEBUG: bool = os.getenv("API_DEBUG", "false").lower() == "true"
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    print(os.getenv("CORS_ORIGINS", "http://localhost:5173"))
    CORS_ORIGINS: List[str] = _split_csv(os.getenv("CORS_ORIGINS", "http://localhost:5173"))
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "ran_quality")
    POSTGRES_USERNAME: str = os.getenv("POSTGRES_USERNAME", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "changeme")

    ROOT_DIRECTORY: str = os.getenv("ROOT_DIRECTORY", "")

    ENABLE_NEIGHBORS: bool = os.getenv("ENABLE_NEIGHBORS", "true").lower() == "true"
    NEIGHBOR_SEARCH_RADIUS_KM: float = float(os.getenv("NEIGHBOR_SEARCH_RADIUS_KM", "3"))


settings = Settings()
