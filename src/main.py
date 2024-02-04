from prefect import flow

from datasource import load_format_articles, load_format_trials, load_format_drugs
from drug_mentions import gather_mentions
from graph_builder import build_link_graph


@flow(name="Drug Mentions Analysis Flow", log_prints=True)
def main():
    articles = load_format_articles(articles_dir="../data/pubmed_files")
    trials = load_format_trials(trials_path="../data/clinical_trials.csv")
    drugs = load_format_drugs(drugs_path="../data/drugs.csv")

    # Get and print the result dictionary
    result_dict = gather_mentions(
        articles_df=articles, trials_df=trials, drugs_df=drugs
    )

    build_link_graph(
        result_dict=result_dict, articles_df=articles,
        trials_df=trials, drugs_df=drugs,
        output_file_path="../data/output/link_graph.json"
    )


if __name__ == "__main__":
    main()
