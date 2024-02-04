from typing import List
from unidecode import unidecode
import pandas as pd


def format_date(df: pd.DataFrame, date_column: str):
    """Converts the date column to datetime format."""
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    return df


def fill_missing_values_duplicates(
        df: pd.DataFrame, columns: List[str], grouped_column: str
):
    """
    Fills missing values in a column with the non-null values found in the trials
    having the exact same title.
    """
    for column in columns:
        df[column] = (
            df.groupby(grouped_column)[column].transform(lambda x: x.bfill().ffill())
        )
    return df


def title_journal_to_lowercase(df: pd.DataFrame):
    """Converts the title and journal columns to lowercase."""
    for column in ["title", "journal"]:
        df[column] = df[column].str.lower()
    return df


def regex_clean(df: pd.DataFrame, columns: List[str]):
    """Cleans the columns using regex patterns."""
    for column in columns:
        df[column] = df[column].replace(r"\\x[0-9a-fA-F]+", "", regex=True)
        df[column] = df[column].apply(unidecode)
    return df
