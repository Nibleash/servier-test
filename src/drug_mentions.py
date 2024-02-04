import pandas as pd
from prefect import task


class DrugMentionsAnalyzer:
    def __init__(
            self, articles_df: pd.DataFrame,
            trials_df: pd.DataFrame, drugs_df: pd.DataFrame
    ):
        self.articles_df = articles_df
        self.trials_df = trials_df
        self.drugs_df = drugs_df
        self.result_dict = {}

    def analyze_drug_mentions(self):
        for df, source in zip([self.articles_df, self.trials_df], ['articles', 'trials']):
            for _, drug_name in self.drugs_df.items():
                print(f"Analyzing drug mentions for {drug_name} in {source}")
                drug_mentions = find_mentions_in_dataframe(df, source, drug_name)

                if drug_name not in self.result_dict:
                    self.result_dict[drug_name] = drug_mentions
                else:
                    self.result_dict[drug_name] = pd.concat(
                        [self.result_dict[drug_name], drug_mentions]
                    )

    def get_result_dictionary(self):
        result_dict = {k: list(map(tuple, v[['title', 'source']].values))
                       for k, v in self.result_dict.items()}
        return result_dict


def find_mentions_in_dataframe(
        df: pd.DataFrame, df_source: str, drug_name: str, title_column: str = "title"
):
    drug_mentions = df[df[title_column].str.contains(
        drug_name, case=False, na=False
    )][[title_column]]
    drug_mentions["source"] = df_source
    drug_mentions["drug"] = drug_name
    return drug_mentions


@task
def gather_mentions(
        articles_df: pd.DataFrame, trials_df: pd.DataFrame, drugs_df: pd.DataFrame
):
    analyzer = DrugMentionsAnalyzer(articles_df, trials_df, drugs_df)
    analyzer.analyze_drug_mentions()
    return analyzer.get_result_dictionary()
