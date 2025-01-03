"""
This file can't import any non standard library, as it's executed in a prefect agent.
"""

from prefect import Task, task
from prefect.blocks.core import Block
import pandera as pa
from abc import ABC, abstractmethod
from pandera.typing import DataFrame, Series
from pandera import DataFrameModel
import pandas as pd
from pydantic import ConfigDict

from tsn_adapters.blocks.github_access import GithubAccess


class PrimitiveSourceDataModel(DataFrameModel):
    stream_id: Series[str]
    source_id: Series[str]
    source_type: Series[str]

    class Config(pa.DataFrameModel.Config):
        strict = "filter"
        coerce = True


class PrimitiveSourcesDescriptor(ABC):
    """
    PrimitiveSourcesDescriptor is a block that describes a source of data.
    It can be a url or a github repository.
    This file can't import any non standard library, as it's executed in a prefect agent.
    """

    model_config = ConfigDict(ignored_types=(Task,))

    @abstractmethod
    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        pass


class UrlPrimitiveSourcesDescriptor(Block, PrimitiveSourcesDescriptor):
    url: str

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        df = pd.read_csv(self.url)
        return DataFrame[PrimitiveSourceDataModel](df)


class GithubPrimitiveSourcesDescriptor(Block, PrimitiveSourcesDescriptor):
    github_access: GithubAccess
    repo: str
    path: str
    branch: str

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        file_content: pd.DataFrame = self.github_access.read_repo_csv_file(
            self.repo, self.path, self.branch
        )
        return DataFrame[PrimitiveSourceDataModel](file_content)


# --- Top Level Task Functions ---
@task(retries=3, retry_delay_seconds=10)
def get_descriptor_from_url(block: UrlPrimitiveSourcesDescriptor) -> DataFrame[PrimitiveSourceDataModel]:
    return block.get_descriptor()

@task(retries=3, retry_delay_seconds=10)
def get_descriptor_from_github(block: GithubPrimitiveSourcesDescriptor) -> DataFrame[PrimitiveSourceDataModel]:
    return block.get_descriptor()


if __name__ == "__main__":
    GithubPrimitiveSourcesDescriptor.register_type_and_schema()
    UrlPrimitiveSourcesDescriptor.register_type_and_schema()
