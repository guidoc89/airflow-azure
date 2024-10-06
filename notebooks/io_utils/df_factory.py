from abc import abstractmethod
from enum import Enum

from .df_abstract_handler import DataFrameAbstractHandler
from .df_names import DataFrameNames
from .df_handlers import CSVHandler, DeltaHandler


class Extension(Enum):
    CSV = "csv"
    DELTA = "delta"

class DataFrameHandlerFactory:

    @abstractmethod
    def get_handler(extension: Extension) -> DataFrameAbstractHandler:
        """
        Returns the handler to use for Databricks spark dataframes I/O operations (load and write) based on format/extension ("csv" or "delta" formats).

        :param name: name of the file/table to save/load to/from Delta Storage. Ex: "df_players.csv" will have as name "df_players"
        :param extension:
        :raises ValueError: not mapped extension
        :return:
        """

        if extension == Extension.CSV:
            return CSVHandler()

        elif extension == Extension.DELTA:
            return DeltaHandler()

        else:
            raise ValueError(f"Unsupported extension: {extension}")
