import re
from pathlib import Path
from hashlib import sha256
from src.epsilon.config.settings import settings
from src.epsilon.domain.dto import RawHTML

def _hash(s: str) -> str:
    return sha256(s.encode()).hexdigest()[:24]

def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")

class HTMLCache:
    def __init__(self, dir: Path | None = None):
        self.dir = (dir or settings.cache_dir)
        self.dir.mkdir(parents=True, exist_ok=True)

    def friendly_path(self, basename: str) -> Path:
        return self.dir / f"{_safe(basename)}.html"

    def exists_friendly(self, basename: str) -> bool:
        return self.friendly_path(basename).exists()

    def write_friendly(self, basename: str, content: str) -> Path:
        p = self.friendly_path(basename)
        p.write_text(content, encoding="utf-8")
        return p

    # === Key-based cache ===
    def key_to_path(self, key: str) -> Path:
        return self.dir / f"{_hash(key)}.html"

    def write_key(self, key: str, content: str) -> Path:
        p = self.key_to_path(key)
        p.write_text(content, encoding="utf-8")
        return p

    def exists_key(self, key: str) -> bool:
        return self.key_to_path(key).exists()

    # (opsional) back-compat untuk kode lama yang pakai url langsung
    def _url_to_path(self, url: str) -> Path:
        return self.key_to_path(url)

    def get(self, url: str) -> RawHTML | None:
        p = self._url_to_path(url)
        if not p.exists():
            return None
        return RawHTML(url=url, content=p.read_text(encoding="utf-8", errors="replace"))

    def put(self, raw: RawHTML) -> None:
        self._url_to_path(raw.url).write_text(raw.content, encoding="utf-8")