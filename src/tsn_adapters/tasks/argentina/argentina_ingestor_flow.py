import datetime
import os
import tempfile
from typing import Any, Dict, Optional
from pandas import DataFrame
from prefect import flow, task
from prefect.context import TaskRunContext
from enum import Enum

from pandera.typing import DataFrame as PaDataFrame

from tsn_adapters.blocks.primitive_source_descriptor import (
    GithubPrimitiveSourcesDescriptor,
    UrlPrimitiveSourcesDescriptor,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
import pandas as pd

from .aggregate.category_price_aggregator import aggregate_prices_by_category

from .models.aggregated_prices import SepaAggregatedPricesModel, sepa_aggregated_prices_to_tn_records

from .models.category_map import SepaProductCategoryMapModel, get_uncategorized_products

from .models.sepa_models import SepaAvgPriceProductModel, SepaProductosDataModel

from .sepa_resource_processor import SepaDirectoryProcessor
from tsn_adapters.tasks.argentina.sepa_scraper import SepaHistoricalDataItem, SepaPreciosScraper
from ..trufnetwork.models.tn_models import TnDataRowModel, TnRecordModel


class PrimitiveSourcesType(Enum):
    URL = "url"
    GITHUB = "github"

    def load_block(self, name: str):
        if self == PrimitiveSourcesType.URL:
            return UrlPrimitiveSourcesDescriptor.load(name)
        elif self == PrimitiveSourcesType.GITHUB:
            return GithubPrimitiveSourcesDescriptor.load(name)
        else:
            raise ValueError(f"Invalid primitive sources type: {self}")

    @staticmethod
    def from_string(value: str):
        if value == "url":
            return PrimitiveSourcesType.URL
        elif value == "github":
            return PrimitiveSourcesType.GITHUB
        else:
            raise ValueError(f"Invalid primitive sources type: {value}")


@flow(log_prints=True)
def argentina_ingestor_flow(
    destination_tn_provider: str, primitive_sources_type: str, tn_access_block_name: str
):
    """

    Flow:
    - get file with argentina streams SourceDescriptor (which may come from UrlSourceDescriptor or GithubSourceDescriptor)
    - read which was the last date of each stream
    - get what dates are available from the source
    - in sequence, for each date, get data from the source, inserting if was missing
    - on aggregator, also output products without corresponding category
    """
    # --- Initialization ---
    # Constants
    argentina_source_name = "argentina_source_descriptor"
    category_map_url = "https://drive.usercontent.google.com/u/2/uc?id=1nfcAjCF-BYU5-rrWJW9eFqCw2AjNpc1x&export=download"
    # list of insertions to wait for
    insertions = []

    # get building blocks
    primitive_sources_block = PrimitiveSourcesType.from_string(
        primitive_sources_type
    ).load_block(argentina_source_name)
    tn_access_block = TNAccessBlock.load(tn_access_block_name)

    # other instances
    scraper = SepaPreciosScraper()
    category_map = SepaProductCategoryMapModel.from_url(category_map_url)

    # --- Main logic ---

    # get primitive sources
    primitive_sources_df = primitive_sources_block.get_descriptor()

    # get last date of each stream
    last_dates_df = pd.DataFrame(columns=["stream_id", "source_id", "last_date"])
    for _, row in primitive_sources_df.iterrows():
        stream_id = row["stream_id"]
        source_id = row["source_id"]
        last_record = tn_access_block.read_records(stream_id=stream_id)
        # may only contain 1 record at most, since we didn't specify a date range
        last_date = last_record["date"][0]
        # but we need to check if there is any at all
        if len(last_record) == 0:
            last_date = "0000-00-00"
        last_dates_df = pd.concat(
            [
                last_dates_df,
                pd.DataFrame(
                    {
                        "stream_id": [stream_id],
                        "source_id": [source_id],
                        "last_date": [last_date],
                    }
                ),
            ]
        )

    # get all dates available from the source
    all_source_data = scraper.scrape_historical_items()
    source_data_by_date = {data_item.date: data_item for data_item in all_source_data}
    sorted_available_dates = sorted(source_data_by_date.keys())

    need_to_process_df = pd.DataFrame(columns=["stream_id", "date_list"])
    for _, row in last_dates_df.iterrows():
        stream_id = row["stream_id"]
        source_id = row["source_id"]
        last_date = row["last_date"]
        # Filter dates that are newer than the last processed date
        dates_to_process = [date for date in sorted_available_dates if date > last_date]
        need_to_process_df = pd.concat(
            [
                need_to_process_df,
                pd.DataFrame(
                    {
                        "stream_id": [stream_id],
                        "source_id": [source_id],
                        "date_list": [dates_to_process],
                    }
                ),
            ]
        )

    # --- Get what dates are needed to be processed ---
    needed_dates_set = set()
    for index, row in need_to_process_df.iterrows():
        stream_id = row["stream_id"]
        source_id = row["source_id"]
        date_list = row["date_list"]
        for date in date_list:
            needed_dates_set.add(date)

    def get_stream_id(source_id: str) -> str:
        result = primitive_sources_df.loc[primitive_sources_df["source_id"] == source_id, "stream_id"].values[0]
        if result is None:
            raise ValueError(f"No stream_id found for source_id: {source_id}")
        return result

    # --- aggregate all data for each date ---
    tn_records_df = PaDataFrame[TnDataRowModel]()
    for date in needed_dates_set:
        date_data_item = source_data_by_date[date]
        date_data = task_get_sepa_data(date_data_item)
        avg_price_product_df = SepaAvgPriceProductModel.from_sepa_product_data(date_data)
        aggregated_data_for_date = aggregate_prices_by_category(category_map, avg_price_product_df)
        tn_records = sepa_aggregated_prices_to_tn_records(aggregated_data_for_date, lambda category_id: category_id)
        tn_records_df = pd.concat([tn_records_df, tn_records])

    # --- insert aggregated data into TSN, for each stream ---
    for _, row in need_to_process_df.iterrows():
        stream_id = row["stream_id"]
        tn_records_for_stream = tn_records_df[tn_records_df["stream_id"] == stream_id]
        insertion = tn_access_block.task_insert_records_and_wait_for_tx(stream_id=stream_id, records=tn_records_for_stream)
        insertions.append(insertion)

    # --- detect uncategorized products ---

    for date in needed_dates_set:
        date_data_item = source_data_by_date[date]
        date_data = task_get_sepa_data(date_data_item)
        # get the products without category
        uncategorized_products = get_uncategorized_products(date_data, category_map)

    report_uncategorized_products(uncategorized_products)

    # Wait for all the insertions to complete
    for future in insertions:
        future.result()
 

def data_item_cache_key(ctx: TaskRunContext, args: Dict[str, Any]) -> Optional[str]:
    data_item: SepaHistoricalDataItem = args['data_item']
    return data_item.date

@task(cache_key_fn=data_item_cache_key))
def task_get_sepa_data(data_item: SepaHistoricalDataItem) -> PaDataFrame[SepaProductosDataModel]:
    with tempfile.TemporaryDirectory() as temp_workdir:
        temp_zip_path = os.path.join(temp_workdir, "data.zip")
        temp_extracted_path = os.path.join(temp_workdir, "extracted")
        # fetch into a temp dir
        data_item.fetch_into_file(temp_zip_path)
        # extract the zip
        processor = SepaDirectoryProcessor.from_zip_path(temp_zip_path, temp_extracted_path)
    return processor.get_all_products_data_merged()


def report_uncategorized_products(uncategorized_products: PaDataFrame[SepaProductosDataModel]):
    print(uncategorized_products)


if __name__ == "__main__":
    argentina_ingestor_flow()

