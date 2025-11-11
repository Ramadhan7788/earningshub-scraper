from __future__ import annotations
import logging, time
from pathlib import Path
from typing import Dict, Iterable, Literal

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from src.epsilon.config.settings import settings
from src.epsilon.services.scraper.base import BaseScraper
from .steps import OpenAnalystRatingsStep, StepReport
from src.epsilon.services.scraper.helpers import LazyLoadScrollStep

log = logging.getLogger(__name__)

VARIANT_OVERVIEW = "overview"
VARIANT_FULL     = "full"

VARIANT_EARNINGS = "earnings"
VARIANT_ANALYST  = "analyst"

AllowedVariant = Literal["overview", "full"]
ALLOWED = {VARIANT_OVERVIEW, VARIANT_FULL}

def _no_results(html: str) -> bool:
    return "symbol not found" in html.lower()

class EarningsHubModularScraper(BaseScraper):
    source = "earningshub"

    def __init__(self, cache_dir: Path | None = None):
        self.cache_dir = cache_dir or settings.cache_dir
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)

    # ---- driver ----
    def _create_driver(self) -> WebDriver:
        opts = Options()
        if settings.selenium_headless:
            opts.add_argument("--headless=new")
            opts.add_argument("--window-size=1920,1080")
        opts.add_argument(f"--user-agent={settings.user_agent}")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        service = Service(executable_path=settings.chrome_driver_path) if settings.chrome_driver_path else Service()
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(settings.selenium_timeout)
        if getattr(settings, "selenium_maximize", False) and not settings.selenium_headless:
            try:
                driver.maximize_window()
            except Exception:
                pass
        return driver

    def _navigate(self, driver: WebDriver, url: str) -> None:
        log.info("Navigating to %s", url)
        driver.get(url)

        WebDriverWait(driver, settings.selenium_timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        WebDriverWait(driver, settings.selenium_timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(0.5)

    # ---- friendly cache helpers ----
    def _friendly_path(self, ticker: str, variant: str) -> Path:
        safe = ticker.strip().upper()
        return Path(self.cache_dir) / f"{safe}_{variant}.html"

    def _exists(self, ticker: str, variant: str) -> bool:
        return self._friendly_path(ticker, variant).exists()

    def _write(self, ticker: str, variant: str, html: str) -> Path:
        p = self._friendly_path(ticker, variant)
        p.write_text(html, encoding="utf-8")
        return p

    # ---- public API ----
    def fetch(self, url: str) -> Path:
        raise NotImplementedError("Use fetch_variants(url, [variant], name_prefix=ticker).")

    def fetch_variants(
        self,
        url: str,
        variants: Iterable[AllowedVariant],
        *,
        single_session: bool = True,
        name_prefix: str | None = None,
        max_analyst_refresh: int = 1,
    ) -> Dict[str, Path]:
        
        order = [v for v in dict.fromkeys(variants)]
        if not order or any(v not in ALLOWED for v in order):
            raise ValueError(f"variants must be subset of {ALLOWED}, got {order}")
        if not name_prefix:
            raise ValueError("name_prefix (ticker) is required")

        want_overview = (VARIANT_OVERVIEW in order) or (VARIANT_FULL in order)
        want_earnings  = (VARIANT_FULL in order)

        results: Dict[str, Path] = {}

        if VARIANT_FULL in order:
            if (self._exists(name_prefix, VARIANT_OVERVIEW)
                and self._exists(name_prefix, VARIANT_ANALYST)
                and self._exists(name_prefix, VARIANT_EARNINGS)):
                results[VARIANT_OVERVIEW] = self._friendly_path(name_prefix, VARIANT_OVERVIEW)
                results[VARIANT_ANALYST]  = self._friendly_path(name_prefix, VARIANT_ANALYST)
                results[VARIANT_EARNINGS] = self._friendly_path(name_prefix, VARIANT_EARNINGS)
                log.info("Served FULL from friendly cache.")
                return results
        else:
            if self._exists(name_prefix, VARIANT_OVERVIEW) and self._exists(name_prefix, VARIANT_ANALYST):
                results[VARIANT_OVERVIEW] = self._friendly_path(name_prefix, VARIANT_OVERVIEW)
                results[VARIANT_ANALYST]  = self._friendly_path(name_prefix, VARIANT_ANALYST)
                log.info("Served OVERVIEW from friendly cache.")
                return results

        driver = None
        try:
            driver = self._create_driver()
            wait = WebDriverWait(driver, settings.selenium_timeout)
            self._navigate(driver, url)

            if want_overview:
                if not self._exists(name_prefix, VARIANT_OVERVIEW):
                    time.sleep(1)
                    LazyLoadScrollStep().run(driver, wait)
                    html = driver.page_source
                    if not _no_results(html):
                        results[VARIANT_OVERVIEW] = self._write(name_prefix, VARIANT_OVERVIEW, html)
                        log.info("Saved overview -> %s", results[VARIANT_OVERVIEW])
                    else:
                        log.info("Skip overview: empty page")
                
                if not self._exists(name_prefix, VARIANT_ANALYST):
                    analysts_url = url.rstrip("/") + "/analysts"
                    self._navigate(driver, analysts_url)
                    try:
                        OpenAnalystRatingsStep().run(driver, wait)
                    except Exception:
                        pass
                    time.sleep(0.3)
                    html = driver.page_source
                    if not _no_results(html):
                        results[VARIANT_ANALYST] = self._write(name_prefix, VARIANT_ANALYST, html)
                        log.info("Saved analyst ratings -> %s", results[VARIANT_ANALYST])
                    else:
                        log.info("Skip analyst ratings: empty page")

            if want_earnings and not self._exists(name_prefix, VARIANT_EARNINGS):
                earnings_url = url.rstrip("/") + "/earnings"
                self._navigate(driver, earnings_url)
                try:
                    LazyLoadScrollStep().run(driver, wait)
                except Exception:
                    pass
                time.sleep(0.3)
                html = driver.page_source
                if not _no_results(html):
                    results[VARIANT_EARNINGS] = self._write(name_prefix, VARIANT_EARNINGS, html)
                    log.info("Saved earnings -> %s", results[VARIANT_EARNINGS])
                else:
                    log.info("Skip earnings: empty page")
                
            if VARIANT_FULL in order:
                results.setdefault(VARIANT_OVERVIEW, self._friendly_path(name_prefix, VARIANT_OVERVIEW))
                results.setdefault(VARIANT_ANALYST,  self._friendly_path(name_prefix, VARIANT_ANALYST))
                results.setdefault(VARIANT_EARNINGS, self._friendly_path(name_prefix, VARIANT_EARNINGS))

        finally:
            if driver:
                try:
                    time.sleep(1)
                    driver.quit()
                except Exception:
                    pass

        return results