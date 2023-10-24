import streamlit as st
from er_evaluation.search import ElasticSearch
from extraction import run_extraction
import pandas as pd
import tempfile
from dotenv import dotenv_values

DF_COLS = {
    "none": [],
    "elastic": {
        'assignee_organization': 'assignees.assignee_organization',
        'assignee_individual_name_last': 'assignees.assignee_individual_name_last',
        'assignee_individual_name_first': 'assignees.assignee_individual_name_first',
        'assignee_country': 'assignees.assignee_country',
        'assignee_state': 'assignees.assignee_state',
        'assignee_city': 'assignees.assignee_city',
        'assignee_type': 'assignees.assignee_type',
        'assignee_id': 'assignees.assignee_id',
        '_score': '_score',
    },
    "sql": {
        'organization': 'organization',
        'name_last': 'name_last',
        'name_first': 'name_first',
        'assignee_country': 'assignee_country',
        'assignee_state': 'assignee_state',
        'assignee_city': 'assignee_city',
        'assignee_type': 'assignee_type',
        'disambiguated_assignee_id': 'disambiguated_assignee_id',
    },
}

#Processing methods
def establish_connection():
    config = dotenv_values(".env")
    es_host = config['es_host']
    api_key = config['es_api_key']
    es = ElasticSearch(es_host, api_key=api_key)
    return es

def parse_csv(csv):
    return [x.strip() for x in csv.split(",")]

def parse_results(results):
    agg_buckets = results["aggregations"][agg_fields[0]][f"{agg_fields[0]}_inner"]["buckets"]
    df = pd.DataFrame.from_records(x["top_hits"]["hits"]["hits"][0]["_source"] for x in agg_buckets)
    df["_score"] = [x["top_hits"]["hits"]["hits"][0]["_score"] for x in agg_buckets]
    df.sort_values("_score", ascending=False, inplace=True)
    return df

@st.cache_data
def search(user_query, index, fields, agg_fields, source, agg_source, timeout, size, fuzziness):
    return es.search(user_query, index, fields, agg_fields=agg_fields, source=source,
                        agg_source=agg_source,
                        timeout=timeout, size=size, fuzziness=fuzziness)


with st.sidebar:

    # Information expander and sidebar
    with st.expander("Information"):
        st.info("This is a demo search tool for disambiguated assignees. By default, the search is performed on the "
            "`assignees.assignee_organization` field, \
            aggregates by disambiguated assignee ID, and returns assignee information for the top hit within each "
            "aggregation bucket.")

        st.info("Aggregation searches can be time-consuming. Avoid including short keywords that may match a large number "
            "of companies (e.g., 'LLC' or 'Corp'). \
            If needed, increase the search timeout to up to a few minutes.")

    with st.expander("Configuration", expanded=True):
        timeout = st.number_input("Timeout", value=30, help="Search timeout in seconds.")
        index = st.text_input("Index", value="patents", help="Index to search in.")
        fuzziness = st.number_input("Fuzziness", value=2, help="Fuzziness level for matching.", min_value=0,
                                    max_value=2)
        col_select_placeholder = st.empty()

    with st.expander("Search Fields (comma separated):", expanded=False):
        # source = parse_csv(st.text_input("Source", value="", help="Fields to return in the response.", ))
        agg_fields = parse_csv(
            st.text_input("Aggregation Fields", value="assignees.assignee_id", help="Fields to aggregate on."))
        agg_source = parse_csv(st.text_input("Aggregation Source", value="assignees",
                                             help="Fields to return for each top hit in the aggregations."))

# Mention ID
col1, col2 = st.columns([1, 2])
mention_id = col1.text_input(label="Mention ID", placeholder="Paste Mention ID Here", value="", label_visibility="collapsed")
patent_url = ("https://patents.google.com/patent/" + mention_id.split("-")[0]) if len(mention_id) > 0 else "https://patents.google.com/"
col2.link_button(label="Go to patent", url=patent_url)

# Input query and processing
es = establish_connection()
user_query = st.text_input("Search:", value="Lutron Electronics")
field_options = ["Organization", "First Name", "Last Name"]
field_select = st.radio("Fields:", field_options, horizontal=True, label_visibility="collapsed")
fields = [list(DF_COLS["elastic"].values())[field_options.index(field_select)]]

# Execute search
try:
    results = search(user_query, index, fields, agg_fields=agg_fields,
                    source=list(DF_COLS["elastic"].values()),
                    agg_source=agg_source,
                    timeout=timeout, size=0, fuzziness=fuzziness)
except Exception as e:
    st.error("Could not complete the search!", icon="🚨")
    st.error(e)
    st.stop()




# Parse results into dataframe
df = parse_results(results)
col_select = col_select_placeholder.multiselect("Columns to display:", options=df.columns, default=DF_COLS["elastic"].keys())
if 'selected_search_results' not in st.session_state: # Record all search results and user selections
    st.session_state.selected_search_results = []

# Create editable table with select column and select all/none feature
@st.cache_data
def generate_table(user_query, toggle, selected_search_results):
    df["Select"] = toggle
    for result in selected_search_results:
        if result["user_query"]==user_query:
            # result["selected_ids"]
            df["Select"] = True
    return df[["Select"]+col_select]

# Enable user selection
df.insert(0, "Select", False)
toggle = st.checkbox("Select all/none")
edited_df = st.data_editor(generate_table(user_query, toggle, st.session_state.selected_search_results))

# Search statistics
entity_count = len(results["aggregations"]["assignees.assignee_id"]["assignees.assignee_id_inner"]["buckets"])
record_count = results["aggregations"]["assignees.assignee_id"]["doc_count"]
st.write(f"Found {entity_count} disambiguated assignees with {record_count} associated records.")



# Create a button to update the results with the selected fields
if st.button("Update Assignee IDs"):
    # Remove any results with the same user_query
    st.session_state.selected_search_results = [result for result in st.session_state.selected_search_results if result["user_query"] != user_query]
    selected_ids = df[edited_df["Select"] == True]["assignee_id"].tolist()
    st.session_state.selected_search_results.append({"user_query": user_query, "selected_ids": selected_ids})

# Output disambiguated assignee IDs
disambiguated_assignee_IDs = [i for result in st.session_state.selected_search_results for i in result["selected_ids"]]
st.write("Selected Assignee IDs:", disambiguated_assignee_IDs)
# st.write("Selected Search Results", st.session_state.selected_search_results)


col1, col2, col3 = st.columns([2, 1, 1])
filename_value = (mention_id+".csv") if len(mention_id) > 0 else "MENTION_ID.csv"
filename = col1.text_input(label="Filename", placeholder="Filename", value=filename_value, label_visibility="collapsed")

if col2.button("Extract"):
    with st.spinner('Extracting...'):
        # if filename[-5:]==".xlsx":
        #     with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
        #         run_extraction(assignee_IDs=disambiguated_assignee_IDs, output_path=temp.name, merge=True)
        #         with open(temp.name, "rb") as file:
        #             bytes_data = file.read()
        #     mime="application/vnd.ms-excel"
        if filename[-4:]==".csv":
            with tempfile.NamedTemporaryFile(suffix=".csv") as temp:
                run_extraction(assignee_IDs=disambiguated_assignee_IDs, output_path=temp.name, simplified=True)
                with open(temp.name, "rb") as file:
                    bytes_data = file.read()
            mime="text/csv"
    col3.download_button(label="Download", data=bytes_data, file_name=filename, mime=mime)