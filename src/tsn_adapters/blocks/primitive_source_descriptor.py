from abc import ABC, abstractmethod
import gzip
from io import BytesIO
from typing import cast

import pandas as pd
import pandera as pa
from pandera import DataFrameModel
from pandera.typing import DataFrame, Series
from prefect import Task, task
from prefect.blocks.core import Block
from prefect.logging import get_run_logger
from prefect_aws import S3Bucket
from pydantic import ConfigDict

from tsn_adapters.blocks.github_access import GithubAccess
from tsn_adapters.utils.deroutine import deroutine


class PrimitiveSourceDataModel(DataFrameModel):
    stream_id: Series[str]
    source_id: Series[str]
    source_type: Series[str]

    class Config(pa.DataFrameModel.Config):
        strict = "filter"
        coerce = True


"""
Blocks that describe a source of data
"""


class PrimitiveSourcesDescriptorBlock(Block, ABC):
    """
    PrimitiveSourcesDescriptor is a block that describes a source of data.
    It can be a url or a github repository.
    This file can't import any non standard library, as it's executed in a prefect agent.
    """

    model_config = ConfigDict(ignored_types=(Task,))

    @abstractmethod
    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        pass


class UrlPrimitiveSourcesDescriptor(PrimitiveSourcesDescriptorBlock):
    url: str

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        df = pd.read_csv(self.url)
        return DataFrame[PrimitiveSourceDataModel](df)


class GithubPrimitiveSourcesDescriptor(PrimitiveSourcesDescriptorBlock):
    github_access: GithubAccess
    repo: str
    path: str
    branch: str

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        file_content: pd.DataFrame = self.github_access.read_repo_csv_file(self.repo, self.path, self.branch)
        return DataFrame[PrimitiveSourceDataModel](file_content)


"""
Writable blocks

Blocks that describe a source of data that can be written to
"""


class WritableSourceDescriptorBlock(PrimitiveSourcesDescriptorBlock):
    @abstractmethod
    def set_sources(self, descriptor: DataFrame[PrimitiveSourceDataModel]):
        pass


class S3SourceDescriptor(WritableSourceDescriptorBlock):
    _block_type_name = "S3 Source Descriptor"

    s3_bucket: S3Bucket
    file_path: str

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            self._logger = get_run_logger()
        return self._logger

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        try:
            file_content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = BytesIO(file_content)
            df = pd.read_csv(
                buffer,
                compression="gzip",
                encoding="utf-8",
                dtype={"stream_id": str, "source_id": str, "source_type": str},
                keep_default_na=False,
                na_values=[],
            )
            df.iloc[27840:27850]
            return DataFrame[PrimitiveSourceDataModel](df)
        except Exception as e:
            self.logger.error(f"Error reading file {self.file_path}: {e}")
            empty_df = pd.DataFrame(columns=list(PrimitiveSourceDataModel.__fields__.keys()))
            return cast(DataFrame[PrimitiveSourceDataModel], empty_df)

    def set_sources(self, descriptor: DataFrame[PrimitiveSourceDataModel]):
        csv_bytes = descriptor.to_csv(index=False, encoding="utf-8").encode("utf-8")
        compressed_bytes = gzip.compress(csv_bytes)
        self.s3_bucket.write_path(
            path=self.file_path,
            content=compressed_bytes,
        )


# --- Top Level Task Functions ---
@task(retries=3, retry_delay_seconds=10)
def get_descriptor_from_url(
    block: UrlPrimitiveSourcesDescriptor,
) -> DataFrame[PrimitiveSourceDataModel]:
    return block.get_descriptor()


@task(retries=3, retry_delay_seconds=10)
def get_descriptor_from_github(
    block: GithubPrimitiveSourcesDescriptor,
) -> DataFrame[PrimitiveSourceDataModel]:
    return block.get_descriptor()


if __name__ == "__main__":
    GithubPrimitiveSourcesDescriptor.register_type_and_schema()
    UrlPrimitiveSourcesDescriptor.register_type_and_schema()
