from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Tuple
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

import time, logging

log = logging.getLogger(__name__)

@dataclass
class StepReport:
    name: str
    present: bool
    clicked: int = 0
    notes: str = ""

class Step(Protocol):
    name: str
    def run(self, driver: WebDriver, wait: WebDriverWait) -> StepReport: ...

def scroll_to_bottom(driver: WebDriver, pause: float = 0.5, max_attempts: int = 3) -> int:
    last_h = driver.execute_script("return document.body.scrollHeight")
    scrolled = 0
    for attempt in range(max_attempts):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        scrolled += 1
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            log.debug("Scroll stable after %s attempt(s).", attempt + 1)
            break
        last_h = new_h
    return scrolled

@dataclass
class LazyLoadScrollStep:
    name: str = "lazy_scroll"
    pause: float = 0.5
    max_attempts: int = 3

    def run(self, driver: WebDriver, wait: WebDriverWait) -> StepReport:
        try:
            actual_scrolls = scroll_to_bottom(
                driver,
                pause=self.pause,
                max_attempts=self.max_attempts
            )
            notes = f"scrolled={actual_scrolls}/{self.max_attempts}"

            return StepReport(name=self.name, present=True, notes=notes)
        except Exception as e:
            return StepReport(name=self.name, present=False, notes=str(e))
        
def bring_into_view_and_click(
    driver: WebDriver, wait: WebDriverWait, locator: Tuple[str, str], attempts: int = 2
) -> tuple[bool, bool]:
    present = False
    for _ in range(attempts):
        try:
            el = wait.until(EC.element_to_be_clickable(locator))
            present = True
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
            time.sleep(0.15)
            try:
                ActionChains(driver).move_to_element(el).pause(0.05).perform()
            except Exception:
                pass
            try:
                el.click()
                return True, True
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", el)
                    return True, True
                except Exception:
                    pass
        except (StaleElementReferenceException, TimeoutException):
            pass
        time.sleep(0.2)
    return present, False