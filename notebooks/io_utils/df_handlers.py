
from pyspark.sql import DataFrame, SparkSession

from .paths import Paths
from .df_names import DataFrameNames
from .df_abstract_handler import DataFrameAbstractHandler



class CSVHandler(DataFrameAbstractHandler):
    def save(self, path: Paths, df: DataFrame, name: DataFrameNames) -> None:
        df.write.mode("overwrite").option("header", "true").format("csv").save(f"{path.value}/{name.value}.csv")

    def load(self, spark: SparkSession, path: Paths | list[str], name: DataFrameNames) -> DataFrame:
        """
        Load a csv file, or a list of csv files (which will get concatenated into 1 spark DataFrame).

        When providing a list, "name" will be ignored.

        :param spark: session.
        :param path: single path (for single file) or list[Paths] of full paths when joining multiple csv files into 1 df when loading.
        :param name: to use as file name. Example: in "df_players.csv", it would be "df_players". Gets ignored when "path" is a list.
        :raises ValueError: 
        :return: 
        """
        if isinstance(path, list):
            return spark.read.format("csv").option("header", "true").load(path)
        elif isinstance(path, Paths):
            return spark.read.format("csv").option("header", "true").load(f"{path.value}/{name.value}.csv")
        else:
            raise ValueError(f"Invalid type '{type(path)}' for path, must be one of Paths or list.")


class DeltaHandler(DataFrameAbstractHandler):
    # NOTE: Delta format doesnt need an extension at the end
    def save(self, path: Paths, df: DataFrame, name: DataFrameNames) -> None:
        df.write.mode("overwrite").format("delta").save(f"{path.value}/{name.value}")

    def load(self, spark: SparkSession, path: Paths, name: DataFrameNames) -> DataFrame:
        return spark.read.format("delta").load(f"{path.value}/{name.value}")
