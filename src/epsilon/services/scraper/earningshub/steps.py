from __future__ import annotations
import time, logging
from dataclasses import dataclass
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

from src.epsilon.services.scraper.helpers import StepReport, bring_into_view_and_click

log = logging.getLogger(__name__)

# ---------------- steps ---------------- #

@dataclass
class OpenAnalystRatingsStep:
    """Open Analyst Ratings section and set to 3 Months if control exists."""
    name: str = "analyst"

    def run(self, driver: WebDriver, wait: WebDriverWait) -> StepReport:
        # set duration to 3 Months if control exists
        combo_present, combo_clicked = bring_into_view_and_click(
            driver, wait, (By.XPATH, "//div[@role='combobox' and contains(normalize-space(.), 'Months')]")
        )
        if combo_present and combo_clicked:
            time.sleep(0.15)
            _p, _c = bring_into_view_and_click(driver, wait, (By.XPATH, "//li[normalize-space(.)='3 Months']"))
            if _p and _c:
                return StepReport(self.name, present=True, clicked=1)
        return StepReport(self.name, present=True, clicked=int(combo_clicked), notes="no_duration_control")
