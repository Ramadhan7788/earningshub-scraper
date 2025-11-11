from abc import ABC, abstractmethod
from src.epsilon.domain.dto import RawHTML

class BaseScraper(ABC):
    source: str
    @abstractmethod
    def fetch(self, url: str) -> RawHTML: ...