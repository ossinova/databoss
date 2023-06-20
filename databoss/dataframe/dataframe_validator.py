import copy
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class DataFrameMissingColumnError(ValueError):
    """raise this when there's a DataFrame column error"""


class DataFrameMissingStructFieldError(ValueError):
    """raise this when there's a DataFrame column error"""


class DataFrameProhibitedColumnError(ValueError):
    """raise this when a DataFrame includes prohibited columns"""


def validate_presence_of_columns(df: DataFrame, required_col_names: List[str]) -> None:
    """Validates the presence of column names in a DataFrame.

    :param df: A spark DataFrame.
    :type df: DataFrame`
    :param required_col_names: List of the required column names for the DataFrame.
    :type required_col_names: :py:class:`list` of :py:class:`str`
    :return: None.
    :raises DataFrameMissingColumnError: if any of the requested column names are
    not present in the DataFrame.
    """
    all_col_names = df.columns
    missing_col_names = [x for x in required_col_names if x not in all_col_names]
    error_message = "The {missing_col_names} columns are not included in the DataFrame with the following columns {all_col_names}".format(
        missing_col_names=missing_col_names, all_col_names=all_col_names
    )
    if missing_col_names:
        raise DataFrameMissingColumnError(error_message)