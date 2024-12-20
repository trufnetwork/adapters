"""
This file can't import any non standard library, as it's executed in a prefect agent.
"""

from prefect.blocks.core import Block
import io
from abc import ABC, abstractmethod
from pandera.typing import DataFrame, Series
from pandera import DataFrameModel
import pandas as pd

from tsn_adapters.blocks.github_access import GithubAccess


class PrimitiveSourceDataModel(DataFrameModel):
    stream_id: Series[str]
    source_id: Series[str]
    source_type: Series[str]

    class Config:
        strict = "filter"


class PrimitiveSourcesDescriptor(Block, ABC):
    """
    PrimitiveSourcesDescriptor is a block that describes a source of data.
    It can be a url or a github repository.
    This file can't import any non standard library, as it's executed in a prefect agent.
    """

    @abstractmethod
    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        pass


class UrlPrimitiveSourcesDescriptor(PrimitiveSourcesDescriptor):
    url: str

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        df = pd.read_csv(self.url)
        return DataFrame[PrimitiveSourceDataModel](df)


class GithubPrimitiveSourcesDescriptor(PrimitiveSourcesDescriptor):
    github_access: GithubAccess
    repo: str
    path: str
    branch: str

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        file_content = self.github_access.read_repo_csv_file(
            self.repo, self.path, self.branch
        )
        return DataFrame[PrimitiveSourceDataModel](file_content)


if __name__ == "__main__":
    PrimitiveSourcesDescriptor.register_type_and_schema()
    GithubPrimitiveSourcesDescriptor.register_type_and_schema()
