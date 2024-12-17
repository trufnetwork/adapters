import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from tsn_adapters.tasks.argentina.scrape_zip import DataItem, SepaPreciosWebScraper

import requests


# Tests for DataItem class
def test_dataitem_valid_date():
    item = DataItem(date="2024-12-16", resource_id="abc123", dataset_id="xyz789")
    assert item.date == "2024-12-16"


def test_dataitem_invalid_date():
    with pytest.raises(ValueError):
        DataItem(date="16/12/2024", resource_id="abc123", dataset_id="xyz789")


def test_dataitem_resource_link():
    item = DataItem(date="2024-12-16", resource_id="abc123", dataset_id="xyz789")
    expected = "https://datos.produccion.gob.ar/dataset/xyz789/archivo/abc123"
    assert item.get_resource_link() == expected


def test_dataitem_download_link():
    # Monday -> lunes
    item = DataItem(date="2024-12-16", resource_id="abc123", dataset_id="xyz789")
    expected = f"{item.get_resource_link()}/download/sepa_lunes.zip"
    assert item.get_download_link() == expected


# Mock HTML for testing the scraper
MOCK_HTML = """
<html>
  <body>
    <ul class="activity" data-module="activity-stream" data-module-more="True" data-module-context="package" data-module-id="6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5" data-module-offset="0" data-total="205">
      <li class="item changed-resource">
        <p>
        Se actualizó el recurso <a href="/dataset/xyz789/archivo/abc123">Lunes</a> en el dataset <span><a href="/dataset/sepa-precios">Precios Claros - Base SEPA</a></span>.
        <span class="date" title="16 Diciembre, 2024, 14:06 (-03)">
            Hace 2 horas.
        </span>
          </p>
        </li>
        <li class="item changed-resource">
        <p>
        Se actualizó el recurso <a href="/dataset/xyz789/archivo/def456">Lunes</a> en el dataset <span><a href="/dataset/sepa-precios">Precios Claros - Base SEPA</a></span>.
        <span class="date" title="17 Diciembre, 2024, 10:00 (-03)">
            Hace 2 horas.
        </span>
          </p>
        </li>
        <li class="load-more"><a href="/dataset/activity/xyz789/30" class="btn btn-rounded">Cargar más</a></li>
    </ul>
  </body>
</html>
"""


# Tests for SepaPreciosWebScraper class
@patch("requests.Session.get")
def test_scraper_parses_items(mock_get):
    mock_response = MagicMock()
    mock_response.text = MOCK_HTML
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosWebScraper()
    items = scraper._get_historical_items()

    assert len(items) == 2
    assert items[0].date == "2024-12-16"
    assert items[0].resource_id == "abc123"
    assert items[0].dataset_id == "xyz789"

    assert items[1].date == "2024-12-17"
    assert items[1].resource_id == "def456"
    assert items[1].dataset_id == "xyz789"


@patch("requests.Session.get")
def test_scraper_no_main_element(mock_get):
    mock_response = MagicMock()
    mock_response.text = "<html></html>"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosWebScraper()
    with pytest.raises(ValueError, match="Main element not found in page"):
        scraper._get_historical_items()


@patch("requests.Session.get")
def test_scraper_no_date_element(mock_get):
    # Missing .date element in one of the items
    mock_response = MagicMock()
    mock_response.text = """
    <html>
      <body>
        <div class="activity">
          <li class="item changed-resource">
          </li>
        </div>
      </body>
    </html>
    """
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosWebScraper()
    with pytest.raises(ValueError, match="Date element not found in item"):
        scraper._get_historical_items()


@patch("requests.Session.get")
def test_scraper_invalid_href(mock_get):
    # Resource href does not match expected pattern
    mock_response = MagicMock()
    mock_response.text = """
    <html>
      <body>
        <div class="activity">
          <li class="item changed-resource">
            <p>
            Se actualizó el recurso <a href="/bad-link/xyz">Lunes</a> en el dataset <span><a href="/dataset/sepa-precios">Precios Claros - Base SEPA</a></span>.
            <span class="date" title="16 Diciembre, 2024, 14:06 (-03)">
                Hace 2 horas.
            </span>
              </p>
          </li>
        </div>
      </body>
    </html>
    """
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosWebScraper()
    with pytest.raises(
        ValueError, match="Error processing item: Resource anchor not found in item"
    ):
        scraper._get_historical_items()


# Integration Tests
@pytest.mark.integration
@pytest.fixture(scope="session")
def scraper_instance():
    """Fixture to create and store the scraper instance and its parsed soup once."""
    scraper = SepaPreciosWebScraper()
    return scraper


@pytest.mark.integration
def test_real_site_has_items(scraper_instance):
    """Test that the real site returns at least one historical item."""
    items = scraper_instance._get_historical_items()
    assert len(items) > 0, "Expected at least one historical item from the live site."

    # Additional validation of the first item
    first_item = items[0]

    # Check date is within reasonable range (not more than 30 days old)
    item_date = datetime.strptime(first_item.date, "%Y-%m-%d")
    today = datetime.now()
    date_diff = today - item_date
    assert date_diff <= timedelta(
        days=30
    ), f"Most recent item date {first_item.date} is too old"


@pytest.mark.integration
def test_download_links_valid(scraper_instance):
    """Test that each item's download link exists and doesn't return an invalid response."""
    items = scraper_instance._get_historical_items()
    session = requests.Session()

    try:
        # We'll just check the first few items to avoid hitting the site too hard
        for item in items[:3]:  # Only check first 3 items
            download_url = item.get_download_link()
            try:
                response = session.head(download_url, allow_redirects=True, timeout=10)

                assert (
                    response.status_code == 200
                ), f"Download link {download_url} returned status {response.status_code}"
            except requests.RequestException as e:
                pytest.fail(f"Failed to check download link {download_url}: {str(e)}")
    finally:
        session.close()


@pytest.mark.integration
def test_resource_links_valid(scraper_instance):
    """Test that each item's resource link exists and is accessible."""
    items = scraper_instance._get_historical_items()
    session = requests.Session()

    try:
        # Check first 3 items
        for item in items[:3]:
            resource_url = item.get_resource_link()
            try:
                response = session.head(resource_url, allow_redirects=True, timeout=10)

                assert (
                    response.status_code == 200
                ), f"Resource link {resource_url} returned status {response.status_code}"
            except requests.RequestException as e:
                pytest.fail(f"Failed to check resource link {resource_url}: {str(e)}")
    finally:
        session.close()

@pytest.mark.integration
def test_specific_date_item(scraper_instance):
    """Test that we can get the first date ever."""
    items = scraper_instance._get_historical_items()
    
    first_date = "2024-11-18"

    # Find item matching target date
    matching_items = [item for item in items if item.date == first_date]
    
    assert len(matching_items) > 0, f"No items found for date {first_date}"
    
    # Verify first matching item has expected date
    item = matching_items[0]
    assert item.date == first_date, \
        f"Expected item date {first_date}, got {item.date}"