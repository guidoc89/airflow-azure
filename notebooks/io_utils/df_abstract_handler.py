from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

from .df_names import DataFrameNames
from .paths import Paths


class DataFrameAbstractHandler(ABC):

    @abstractmethod
    def load(self, spark: SparkSession, path: Paths | list[str], name: DataFrameNames) -> DataFrame:
        pass

    @abstractmethod
    def save(self, path: Paths, df: DataFrame, name: DataFrameNames) -> None:
        pass
