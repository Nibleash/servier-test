import json
from prefect import task


class LinkGraphBuilder:
    def __init__(self, result_dict, articles_df, trials_df, drugs_df):
        self.articles_df = articles_df
        self.trials_df = trials_df
        self.drugs_df = drugs_df
        self.result_dict = result_dict
        self.link_graph = {"nodes": [], "links": []}

    def build_link_graph(self):
        drug_nodes = [
            {"id": drug_name, "type": "drug"} for drug_name in self.result_dict.keys()
        ]
        source_nodes = [
            {"id": "PubMed", "type": "publication_type"},
            {"id": "Clinical Trial", "type": "publication_type"}
        ]
        articles_journal_nodes = [
            {"id": journal, "type": "journal"}
            for journal in self.articles_df["journal"].unique()
        ]
        trials_journal_nodes = [
            {"id": journal, "type": "journal"}
            for journal in self.trials_df["journal"].unique()
        ]

        self.link_graph["nodes"] = (drug_nodes + articles_journal_nodes +
                                    trials_journal_nodes + source_nodes)

        for drug_name, drug_mentions in self.result_dict.items():
            for title, source in drug_mentions:
                if source == "articles":
                    date = self.articles_df.loc[self.articles_df["title"] == title, "date"].values[0]
                    journal = self.articles_df.loc[self.articles_df["title"] == title, "journal"].values[0]
                    publication_type = "PubMed"
                else:
                    date = self.trials_df.loc[self.trials_df["title"] == title, "date"].values[0]
                    journal = self.trials_df.loc[self.trials_df["title"] == title, "journal"].values[0]
                    publication_type = "Clinical Trial"

                date_str = str(date)[:10]
                drug_link = {"source": journal, "target": drug_name, "type": "contain_drug", "date": date_str}
                source_link = {"source": drug_name, "target": publication_type, "type": "mentionned_in", "date": date_str}
                journal_link = {"source": publication_type, "target": journal, "type": "published_in", "date": date_str}

                self.link_graph["links"].append(drug_link)
                self.link_graph["links"].append(source_link)
                self.link_graph["links"].append(journal_link)

    def save_to_json(self, output_file_path):
        with open(output_file_path, "w") as output_file:
            json.dump(self.link_graph, output_file, indent=2)


@task
def build_link_graph(result_dict, articles_df, trials_df, drugs_df, output_file_path):
    builder = LinkGraphBuilder(result_dict, articles_df, trials_df, drugs_df)
    builder.build_link_graph()
    builder.save_to_json(output_file_path)
