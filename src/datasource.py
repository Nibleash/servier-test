import pandas as pd
from prefect import task

from utils.load_file import explore_directory, load_json
from utils.preprocess import format_date, fill_missing_values_duplicates, \
    title_journal_to_lowercase, regex_clean


@task
def load_format_articles(articles_dir: str) -> pd.DataFrame:
    """Explore the directory and load the articles into a DataFrame."""
    articles_files = explore_directory(articles_dir)
    articles_concat = []

    for file_path, file_format in articles_files:
        if file_format == "csv":
            articles_concat.append(pd.read_csv(file_path))
        elif file_format == "json":
            articles_concat.append(load_json(file_path))

    articles = pd.concat(articles_concat, axis=0, ignore_index=True)
    articles = format_date(articles, "date")
    articles.drop(columns=['id'], inplace=True)
    articles = regex_clean(articles, ["title", "journal"])

    return title_journal_to_lowercase(articles)


@task
def load_format_trials(trials_path: str) -> pd.DataFrame:
    """Load and format the trials into a DataFrame."""
    trials = pd.read_csv(trials_path)
    trials.rename(columns={"scientific_title": "title"}, inplace=True)

    trials = format_date(trials, "date")
    trials = fill_missing_values_duplicates(trials, ["journal", "id"], "title")
    trials = regex_clean(trials, ["title", "journal"])

    return title_journal_to_lowercase(trials)


@task
def load_format_drugs(drugs_path: str) -> pd.DataFrame:
    """Load and format the drugs into a DataFrame."""
    drugs = pd.read_csv(drugs_path)
    return drugs['drug'].str.lower()
