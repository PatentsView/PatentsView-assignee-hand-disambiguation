import pandas as pd
import numpy as np
np.random.seed(0)

disamb = pd.read_csv(
    "https://s3.amazonaws.com/data.patentsview.org/download/g_persistent_assignee.tsv.zip",
    dtype=str,
    sep="\t",
    compression="zip")

disamb["mention_id"] = "US" + disamb["patent_id"] + "-" + disamb["assignee_sequence"]
disamb_20220929 = disamb.set_index("mention_id")["disamb_assignee_id_20220929"]
disamb_20220929 = disamb_20220929.dropna()
mention_ids = disamb_20220929.index

sample = np.random.choice(mention_ids, size=10000, replace=False)
pd.Series(sample).to_csv("sample.csv")
