# PatentsView Assignee Hand Disambiguation
Codebase for algorithmically setting up the manual assignee labeling tasks.

## Summary

|  |  |
|---|---|
| DSAA Team Lead | Sarvo Madhavan | 
| Project Tier | Tier 2 | 
| DSAA Team Members | - Olivier Binette (former AIR employee)<br>- Sarvo Madhavan (Senior Data Scientist)
    <br>- Beth Anne Card (Data Scientist) <br>- Siddharth Engineer (Data Scientist Assistant) |
| Client(s) | PatentsView (USPTO) |
| Project Start Date | 07/26/2023 |
| Project End Date | None |
| Status | In progress (awaiting hand-disambiguation process) |                                                                 

 ## Raw Materials In
- Sample of 800 mention IDs from g_persistent_assignee.tsv.zip from https://patentsview.org/download/data-download-tables
- AWS database of PatentsView data for populating sample data
- ElasticSearch for finding similar assignee names and storing potential disambiguated_assignee_ID clusters to merge
- PatentsView API for retrieving data on assignee clusters for removal step

## Result Out
For each mention ID in the generated sample, we will save a Dataframe containing all mention IDs belonging to the same assignee.

## Usage/Examples

First we'll need to set up our various data source connections.
In this directory, create a `.env` file with the following fields:

```
# SQL
user=
password=
hostname="patentsview-ingest-production.cckzcdkkfzqo.us-east-1.rds.amazonaws.com"
dbname="algorithms_assignee_labeling"

# Elastic Search
es_host="https://patentsview-production-0cb426.es.us-east-1.aws.found.io"
es_api_key=

# PV API
pv_api_key=
```

We recommend creating a virtual environment for your work. Run the following commands in this directory.

```
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
```

To generate our list of sample mention IDs, running `sample_mentions()` from the `assignee.py` script.
Note that you'll need to download `g_persistent_assignee.tsv.zip` from https://patentsview.org/download/data-download-tables and for it to be saved in this directory.

To retrieve our samples ready for usage by the hand labelers, use `populate_sample()` and `segment_sample()` from the `assignee.py` script.

To run the streamlit app, type `streamlit run app.py` into your terminal. For a given mention ID, everything is handled in the app from this point on.

## FAQ

#### Q: Does it work well on both Windows and Mac?
A: You're hilarious

## Support
For support, email fake@fake.com for a response that is sure to be real

## License
Apparently this is a thing. 

### Roadmap

- Additional browser support
- Add more integrations
- Cat tree for Sami

## Appendix

Any additional information goes here. Could be related projects, helpful links to packages used, etc.

## How to start the SSH Instance
Connecting to SSH
1. From terminal, type `ssh PV-hand-disamb` to open the SSH connection.
2. Go to the GitHub repository in the streamlit directory.

To start the instance from scratch:
3. Enter the virtual environment with `source env/bin/activate`.
4. Use the `screen` command to create a screen instance. Recommended to give this instance a name.
5. Run `streamlit run app.py`.
5. Can safely exit.

To re-open an existing screen instance:
3. Use screen commands.