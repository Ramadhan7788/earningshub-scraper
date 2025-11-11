from __future__ import annotations

import logging

from logging.handlers import RotatingFileHandler
from pathlib import Path
from pydantic import Field
from pydantic_settings import SettingsConfigDict, BaseSettings

class Settings(BaseSettings):

    app_env: str = "dev"
    rate_limit: int = 60

    chrome_driver_path: str | None = None
    selenium_headless: bool = True
    selenium_timeout: int = 30
    selenium_maximize: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )

    data_dir: Path = Field(default_factory=lambda: Path("data"))
    
    cache_dir: Path | None = None
    log_dir: Path | None = None

    log_level: str = "INFO"
    log_to_console: bool = True
    log_to_file: bool = True
    log_file_name: str = "app.log"
    log_format: str = (
        "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
    )
    log_rotate_bytes: int = 2 * 1024 * 1024 # 2 MB
    log_backup_count: int = 3               # keep 3 old log files 
    
    # Database configuration
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = "root123"
    database: str = "epsilon"
    table: str = "earnings"

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
    )

    def model_post_init(self, __context) -> None:
        # Set default paths if not provided
        if self.cache_dir is None:
            self.cache_dir = self.data_dir / "html_cache"
        if self.log_dir is None:
            self.log_dir = self.data_dir / "logs"

        # Ensure directories exist
        for p in (self.data_dir, self.cache_dir, self.log_dir):
            try:
                Path(p).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"[settings] Failed to create directory {p}: {e}")

        self.configure_logging()

    def configure_logging(self) -> None:
        root = logging.getLogger()
        if getattr(root, "_epsilon_configured", False):
            return
        
        if root.hasHandlers:
            for h in list(root.handlers):
                root.removeHandler(h)
        
        root.setLevel(self.log_level.upper())
        formatter = logging.Formatter(self.log_format)

        if self.log_to_console:
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            root.addHandler(ch)

        if self.log_to_file:
            log_path = Path(self.log_dir) / self.log_file_name
            fh = RotatingFileHandler(
                log_path,
                maxBytes=self.log_rotate_bytes,
                backupCount=self.log_backup_count,
                encoding="utf-8"
            )
            fh.setFormatter(formatter)
            root.addHandler(fh)
        
        root._epsilon_configured = True

settings = Settings()