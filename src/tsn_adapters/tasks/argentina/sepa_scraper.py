import logging
import re
from datetime import datetime
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from prefect import task, get_run_logger
from prefect.context import get_run_context, TaskRunContext
from tqdm import tqdm
import time

from .utils.dates import date_to_weekday
from pydantic import BaseModel, field_validator


class SepaHistoricalDataItem(BaseModel):
    """
    Represents a historical data item from the dataset.
    """
    # not necessarily the website date corresponds to the real date inside the zip file
    # e.g. they may report at the website to have updated on a certain date, but the real date inside the zip file is different
    website_date: str
    resource_id: str
    dataset_id: str
    _base_url = "https://datos.produccion.gob.ar/dataset"

    @field_validator('website_date')
    @classmethod
    def validate_date(cls, v: str) -> str:
        # Strict check for YYYY-MM-DD
        datetime.strptime(v, "%Y-%m-%d")
        return v

    def get_resource_link(self) -> str:
        """
        Returns the resource page link in the old 'archivo' pattern.
        e.g. https://datos.produccion.gob.ar/dataset/<dataset_id>/archivo/<resource_id>
        """
        return f"{self._base_url}/{self.dataset_id}/archivo/{self.resource_id}"

    def get_download_link(self) -> str:
        """
        Returns the direct download link in the 'resource' pattern, including the weekday in Spanish.
        e.g. https://datos.produccion.gob.ar/dataset/<dataset_id>/resource/<resource_id>/download/sepa_<weekday>.zip
        """
        lowercase_weekday = date_to_weekday(self.website_date).lower()
        return (
            f"{self._base_url}/{self.dataset_id}/resource/{self.resource_id}/download/sepa_{lowercase_weekday}.zip"
        )

    def fetch_into_memory(self, show_progress_bar: bool = False) -> bytes:
        """
        Fetch the data into memory, optionally showing a progress bar,
        and gracefully handle cancellation.
        
        Returns:
            bytes: The complete file content, or raises an exception if download failed/cancelled
        """
        download_link = self.get_download_link()
        logger = self._get_logger()
        logger.info(f"Starting download from {download_link}")
        
        # Try HEAD request first, but don't fail if it doesn't work
        total_size: Optional[int] = None
        try:
            head_response = requests.head(download_link, allow_redirects=True, timeout=5)
            if head_response.ok:
                total_size = int(head_response.headers.get('content-length', 0))
                if total_size:
                    logger.info(f"HEAD request successful. Expected file size: {total_size/1024/1024:.2f} MB")
        except Exception as e:
            logger.debug(f"HEAD request failed (this is ok, will proceed with GET): {e}")
        
        # Prepare for streaming download
        chunks: List[bytes] = []
        downloaded_size = 0
        progress_bar = None
        response = None
        last_chunk_time = time.time()
        chunk_timeout = 30  # seconds
        chunk_log_interval = 10 * 1024 * 1024  # Log every 10MB
        next_log_threshold = chunk_log_interval

        try:
            response = requests.get(
                download_link, 
                stream=True, 
                timeout=(10, 5),  
                allow_redirects=True,
                headers={'Accept-Encoding': 'identity'}
            )
            response.raise_for_status()
            
            # Check for Content-Range + 200 OK mismatch
            content_range = response.headers.get("content-range")
            if response.status_code == 200 and content_range:
                # Try to parse the content range header
                range_match = re.match(r"bytes (\d+)-(\d+)/(\d+)", content_range)
                if range_match:
                    start, end, full_size = map(int, range_match.groups())
                    logger.warning(
                        f"Server responded with 200 OK but included Content-Range header. "
                        f"Range: {start}-{end}, Full size: {full_size/1024/1024:.2f}MB. "
                        "This may indicate a server misconfiguration."
                    )
                    # Use the full size from Content-Range if we don't have it yet
                    if not total_size:
                        total_size = full_size
                        logger.info(f"Using size from Content-Range: {total_size/1024/1024:.2f} MB")
                else:
                    logger.warning(
                        f"Server sent invalid Content-Range header with 200 OK: {content_range}"
                    )
            
            # If HEAD failed, try to get size from GET response
            if not total_size:
                total_size = int(response.headers.get('content-length', 0))
                if total_size:
                    logger.info(f"GET request successful. File size: {total_size/1024/1024:.2f} MB")
                else:
                    logger.info("File size unknown (streaming download)")
                    show_progress_bar = False

            if show_progress_bar and total_size:
                progress_bar = tqdm(
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    desc=f"Downloading {download_link}"
                )

            logger.debug("Waiting for first data chunk...")
            retries = 3

            for chunk in response.iter_content(chunk_size=8192):
                current_time = time.time()
                time_since_last_chunk = current_time - last_chunk_time
                
                # Check for stalled download
                if time_since_last_chunk > chunk_timeout:
                    raise requests.exceptions.ReadTimeout(
                        f"No data received for {time_since_last_chunk:.1f} seconds"
                    )
                
                # Check for cancellation
                run_context = get_run_context()
                task_run = run_context.task_run if isinstance(run_context, TaskRunContext) else None
                state = task_run.state if task_run is not None else None
                if state is not None and (state.is_crashed() or state.is_cancelled()):
                    logger.warning("Cancellation detected by Prefect.")
                    raise InterruptedError("Download cancelled by Prefect")

                if not chunk:  # Skip empty chunks
                    continue

                # Update last chunk time
                last_chunk_time = current_time

                # Retry logic for failed chunks
                for attempt in range(retries):
                    try:
                        chunks.append(chunk)
                        downloaded_size += len(chunk)
                        break
                    except Exception as e:
                        if attempt == retries - 1:
                            raise
                        logger.warning(f"Chunk download failed, retrying ({attempt + 1}/{retries}): {e}")
                        time.sleep(1)

                # Update progress
                if progress_bar:
                    progress_bar.update(len(chunk))
                elif downloaded_size >= next_log_threshold:
                    logger.info(
                        f"Downloaded: {downloaded_size/1024/1024:.1f} MB" + 
                        (f" of {total_size/1024/1024:.1f} MB" if total_size else " (unknown total)")
                    )
                    next_log_threshold = downloaded_size + chunk_log_interval

            # Verify download completion
            if total_size and downloaded_size < total_size:
                raise ValueError(
                    f"Incomplete download: got {downloaded_size} bytes, expected {total_size} bytes"
                )

            # Join all chunks into final result
            logger.info(f"Download complete. Total size: {downloaded_size/1024/1024:.1f} MB")
            return b''.join(chunks)

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.error(f"Network error while downloading {download_link}: {e}")
            raise

        except (KeyboardInterrupt, InterruptedError):
            logger.warning("Download interrupted")
            raise

        except Exception as e:
            logger.error(f"Unexpected error during download: {e}")
            raise

        finally:
            if progress_bar:
                progress_bar.close()
            if response:
                response.close()
            # Clear the chunks list to free memory
            chunks.clear()

    def fetch_into_file(self, path: str, show_progress_bar: bool = False) -> None:
        with open(path, "wb") as f:
            f.write(self.fetch_into_memory(show_progress_bar=show_progress_bar))

    def _get_logger(self):
        try:
            logger = get_run_logger()
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to get run logger: {e}")
        return logger


class SepaPreciosScraper:
    """
    Scraper for the structure:
    <div class="pkg-container">
       <div class="pkg-actions">
         <a href="..."><button>CONSULTAR</button></a>
         <a href="..."><button>DESCARGAR</button></a>
       </div>
       <a href="...">
         <div class="package-info">
           <h3>Miércoles</h3>
           <p>Precios SEPA Minoristas miércoles, 2024-12-25</p>
         </div>
         <div class="pkg-file-img" data-format="zip"><p>zip</p></div>
       </a>
    </div>
    """
    main_url = "https://datos.produccion.gob.ar/dataset/sepa-precios"

    def __init__(self, delay_seconds: float = 0.1, show_progress_bar: bool = False):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "SEPA Precios Scraper"})
        self.delay_seconds = delay_seconds
        self.show_progress_bar = show_progress_bar
        self.logger = self._get_logger()

    def _get_logger(self):
        try:
            logger = get_run_logger()
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to get run logger: {e}")
        return logger

    def __del__(self):
        self.session.close()

    def _get_soup(self) -> BeautifulSoup:
        try:
            resp = self.session.get(self.main_url)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {self.main_url}: {e}")
            raise

    def _extract_date_from_text(self, text: str) -> str:
        """
        Look for a pattern like: '..., 2024-12-25' and return '2024-12-25'
        """
        match = re.search(r'\b\d{4}-\d{2}-\d{2}\b', text)
        if not match:
            raise ValueError(f"No valid YYYY-MM-DD date found in text: {text}")
        return match.group(0)

    def _extract_data_item(self, container) -> SepaHistoricalDataItem:
        """
        For each .pkg-container, parse:
         - The date from the .package-info's <p> text
         - The dataset_id and resource_id from the "DESCARGAR" link in .pkg-actions
        """
        # 1) Extract the date from the .package-info text.
        package_info = container.select_one(".package-info")
        if not package_info:
            raise ValueError("No .package-info found within .pkg-container")

        text_content = package_info.get_text(" ", strip=True)
        date_str = self._extract_date_from_text(text_content)

        # 2) Find the 'DESCARGAR' link in .pkg-actions
        pkg_actions = container.select_one(".pkg-actions")
        if not pkg_actions:
            raise ValueError("No .pkg-actions found within .pkg-container")

        descargar_link = None
        for a in pkg_actions.find_all("a"):
            btn_text = a.get_text(strip=True).upper()
            if "DESCARGAR" in btn_text:
                descargar_link = a
                break
        if not descargar_link:
            raise ValueError("No DESCARGAR button/link found within .pkg-actions")

        href = (descargar_link.get("href") or "").strip()
        # Example:
        # "/dataset/xyz789/archivo/abc123"
        # or "https://datos.produccion.gob.ar/dataset/xyz789/resource/abc123/download/sepa_miercoles.zip"
        # We'll parse out "xyz789" and "abc123" from that.

        pattern = r'/dataset/(.+?)/(archivo|resource)/([^/]+)'
        match = re.search(pattern, href)
        if not match:
            raise ValueError(f"DESCARGAR link does not match expected pattern: {href}")

        dataset_id = match.group(1)
        resource_id = match.group(3)

        return SepaHistoricalDataItem(
            website_date=date_str,
            resource_id=resource_id,
            dataset_id=dataset_id
        )

    def scrape_historical_items(self) -> List[SepaHistoricalDataItem]:
        soup = self._get_soup()

        # Each resource is in a .pkg-container
        containers = soup.select(".pkg-container")
        if not containers:
            raise ValueError("No .pkg-container elements found.")

        items = []
        for c in containers:
            try:
                data_item = self._extract_data_item(c)
                items.append(data_item)
            except Exception as e:
                # log but continue
                self.logger.warn(f"Failed to parse container: {e}")

        if not items:
            raise ValueError("No valid items extracted from the page.")
        return items


@task(retries=3, retry_delay_seconds=10)
def task_scrape_historical_items(scraper: SepaPreciosScraper) -> List[SepaHistoricalDataItem]:
    return scraper.scrape_historical_items()