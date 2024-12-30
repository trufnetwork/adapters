import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from tsn_adapters.tasks.argentina.sepa_scraper import (
    SepaHistoricalDataItem,
    SepaPreciosScraper
)
import requests

def test_dataitem_valid_date():
    item = SepaHistoricalDataItem(
        date="2024-12-16",
        resource_id="abc123",
        dataset_id="xyz789"
    )
    assert item.date == "2024-12-16"

def test_dataitem_invalid_date():
    with pytest.raises(ValueError, match="does not match format"):
        SepaHistoricalDataItem(
            date="16/12/2024",
            resource_id="abc123",
            dataset_id="xyz789"
        )

def test_dataitem_download_link():
    # Monday -> lunes
    item = SepaHistoricalDataItem(
        date="2024-12-16",
        resource_id="abc123",
        dataset_id="xyz789"
    )
    expected = (
        "https://datos.produccion.gob.ar/dataset/xyz789/resource/"
        "abc123/download/sepa_lunes.zip"
    )
    assert item.get_download_link() == expected


MOCK_HTML = """
<html>
  <body>
    <div class="pkg-container">
      <div class="pkg-actions">
        <a href="/dataset/xyz789/archivo/abc123"><button>CONSULTAR</button></a>
        <a href="https://datos.produccion.gob.ar/dataset/xyz789/resource/abc123/download/sepa_miercoles.zip">
          <button>DESCARGAR</button>
        </a>
      </div>
      <a href="/dataset/sepa-precios/archivo/1e92cd42-4f94-4071-a165-62c4cb2ce23c">
        <div class="package-info">
          <h3>Miércoles</h3>
          <p>Precios SEPA Minoristas miércoles, 2024-12-25</p>
        </div>
        <div class="pkg-file-img" data-format="zip">
          <p>zip</p>
        </div>
      </a>
    </div>

    <div class="pkg-container">
      <div class="pkg-actions">
        <a href="/dataset/xyz789/archivo/def456">
          <button>CONSULTAR</button>
        </a>
        <a href="/dataset/xyz789/resource/def456/download/sepa_jueves.zip">
          <button>DESCARGAR</button>
        </a>
      </div>
      <a href="/dataset/xyz789/archivo/def456">
        <div class="package-info">
          <h3>Jueves</h3>
          <p>Precios SEPA Minoristas jueves, 2024-12-26</p>
        </div>
        <div class="pkg-file-img" data-format="zip">
          <p>zip</p>
        </div>
      </a>
    </div>
  </body>
</html>
"""

@patch("requests.Session.get")
def test_scraper_parses_pkg_containers(mock_get):
    mock_response = MagicMock()
    mock_response.text = MOCK_HTML
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosScraper()
    items = scraper.scrape_historical_items()

    assert len(items) == 2

    # First container: date=2024-12-25, resource_id=abc123, dataset_id=xyz789
    assert items[0].date == "2024-12-25"
    assert items[0].dataset_id == "xyz789"
    assert items[0].resource_id == "abc123"

    # Second container: date=2024-12-26, resource_id=def456, dataset_id=xyz789
    assert items[1].date == "2024-12-26"
    assert items[1].dataset_id == "xyz789"
    assert items[1].resource_id == "def456"


@patch("requests.Session.get")
def test_scraper_no_pkg_containers(mock_get):
    # If no .pkg-container elements, raise
    mock_response = MagicMock()
    mock_response.text = "<html><body>No containers here</body></html>"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosScraper()
    with pytest.raises(ValueError, match="No .pkg-container elements found"):
        scraper.scrape_historical_items()


@patch("requests.Session.get")
def test_scraper_invalid_href(mock_get):
    # If the DESCARGAR link doesn't match /dataset/.../archivo|resource/...,
    # the item won't get added, leading to no items and a ValueError
    bad_html = """
    <html>
      <body>
        <div class="pkg-container">
          <div class="pkg-actions">
            <a href="/strange-link/xyz123"><button>DESCARGAR</button></a>
          </div>
          <a href="/dataset/xyz789/archivo/abc123">
            <div class="package-info">
              <p>Precios SEPA Minoristas viernes, 2024-12-27</p>
            </div>
          </a>
        </div>
      </body>
    </html>
    """
    mock_response = MagicMock()
    mock_response.text = bad_html
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    scraper = SepaPreciosScraper()
    with pytest.raises(ValueError, match="No valid items extracted"):
        scraper.scrape_historical_items()
