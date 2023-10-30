import streamlit as st
from er_evaluation.search import ElasticSearch
from extraction import run_extraction, simple_extraction_output
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
    agg_buckets = results["aggregations"][f"{agg_fields[0]}_inner"]["buckets"]
    df = pd.DataFrame.from_records(x["top_hits"]["hits"]["hits"][0]["_source"] for x in agg_buckets)
    df["_score"] = [x["top_hits"]["hits"]["hits"][0]["_score"] for x in agg_buckets]
    df.sort_values("_score", ascending=False, inplace=True)
    return df

@st.cache_data
def search(user_query, index, fields, agg_fields, source, agg_source, timeout, size, fuzziness):
    return es.search(user_query=user_query, index=index, fields=fields, agg_fields=agg_fields, source=source,
                    agg_source=agg_source, timeout=timeout, size=size, fuzziness=fuzziness)


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
        index = st.text_input("Index", value="assignee_references", help="Index to search in.")
        fuzziness = st.number_input("Fuzziness", value=2, help="Fuzziness level for matching.", min_value=0,
                                    max_value=2)
        col_select_placeholder = st.empty()

    with st.expander("Search Fields (comma separated):", expanded=False):
        source = parse_csv(st.text_input("Source", value="", help="Fields to return in the response.", )) # list(DF_COLS["elastic"].keys()) 
        agg_fields = parse_csv(
            st.text_input("Aggregation Fields", value="assignee_id", help="Fields to aggregate on."))
        agg_source = parse_csv(st.text_input("Aggregation Source", value="",
                                             help="Fields to return for each top hit in the aggregations."))

"""
Mention ID:
"""
@st.cache_data
def mention_id_data(mention_id):
    return simple_extraction_output(mention_id)

col1, col2, col3 = st.columns([3, 1, 1])
mention_id = col1.text_input(label="Mention ID", placeholder="Paste Mention ID Here", value="", label_visibility="collapsed")
patent_id = mention_id.split("-")[0]
if len(mention_id) > 0:
    pv_url = f"https://datatool.patentsview.org/#detail/patent/{patent_id[2:]}/"
    gp_url = f"https://patents.google.com/patent/{patent_id}/"
    st.write(mention_id_data(mention_id))
else:
    pv_url = "https://datatool.patentsview.org/#search&pat=2|"
    gp_url = "https://patents.google.com/"

col2.link_button(label="PatentsView", url=pv_url)
col3.link_button(label="Google Patents", url=gp_url)


"""
Search:
"""
es = establish_connection()
user_query = st.text_input(label="Search", value="", label_visibility="collapsed")
field_options = ["Organization", "First Name", "Last Name"]
field_select = st.radio("Fields:", field_options, horizontal=True, label_visibility="collapsed")
fields = [list(DF_COLS["elastic"].keys())[field_options.index(field_select)]]

# Execute search
try:
    results = search(user_query=user_query, index=index, fields=fields, agg_fields=agg_fields,\
                     source=[], agg_source=[], timeout=timeout, size=0, fuzziness=fuzziness)
except Exception as e:
    st.error("Could not complete the search!", icon="🚨")
    st.error(e)
    st.stop()


# Parse results into dataframe
df = parse_results(results)
col_select = col_select_placeholder.multiselect("Columns to display:", options=df.columns, default=DF_COLS["elastic"].keys())
if 'selected_search_results' not in st.session_state: # Record all search results and user selections
    st.session_state.selected_search_results = []

# Generates editable table with select column reflecting st.session_state.selected_search_results
df.insert(0, "Select", False)
@st.cache_data
def generate_table(user_query, selected_search_results):
    for result in selected_search_results:
        if result["user_query"]==user_query:
            df["Select"] = df["assignee_id"].isin(result["selected_ids"])
    return df[["Select"]+col_select]

# Changes search results field to store the new selected_ids parameter
def update_selection(selected_ids):
    new_query = True
    for result in st.session_state.selected_search_results:
        if result["user_query"]==user_query:
            new_query = False
            result["selected_ids"] = selected_ids
    if new_query:
        st.session_state.selected_search_results.append({"user_query": user_query, "selected_ids": selected_ids, "df": df[col_select].copy()})

# Add/remove all by editing st.session_state.selected_search_results
col1, col2, col3 = st.columns([2, 3, 10])
if col1.button("Add all"):
    update_selection(df["assignee_id"].tolist())
if col2.button("Remove all"):
    update_selection([])

# Enable user selection
edited_df = st.data_editor(generate_table(user_query, st.session_state.selected_search_results))

# Search statistics
entity_count = len(results["aggregations"]["assignee_id_inner"]["buckets"])
st.write(f"Found {entity_count} disambiguated assignees.")


# Create a button to update the results field with the selected fields
if st.button("Update Assignee IDs"):
    selected_ids = df[edited_df["Select"]]["assignee_id"].tolist()
    update_selection(selected_ids)

# Output disambiguated assignee IDs
disambiguated_assignee_IDs = [i for result in st.session_state.selected_search_results for i in result["selected_ids"]]
if len(st.session_state.selected_search_results) > 0:
    selected_df = pd.concat([result["df"][result["df"]['assignee_id'].isin(result["selected_ids"])] for result in st.session_state.selected_search_results])
    selected_df.insert(0, "Remove", False)
    edited_selected_df = st.data_editor(selected_df[["Remove"]+col_select])

    if st.button("Remove Assignee IDs"):
        remove_ids = edited_selected_df[edited_selected_df["Remove"]]["assignee_id"].tolist()
        for result in st.session_state.selected_search_results:
            result["selected_ids"] = [id for id in result["selected_ids"] if id not in remove_ids]

"""
Extraction and Download:
"""
col1, col2, col3, col4 = st.columns([10, 3, 4, 3])
filename_value = (mention_id+".csv") if len(mention_id) > 0 else ""
filename = col1.text_input(label="Filename", placeholder="Filename", value=filename_value, label_visibility="collapsed")

if col2.button("Extract"):
    with st.spinner('Extracting...'):
        if filename[-5:]==".xlsx":
            with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
                run_extraction(assignee_IDs=disambiguated_assignee_IDs, output_path=temp.name, simplified=True)
                with open(temp.name, "rb") as file:
                    bytes_data = file.read()
            mime="application/vnd.ms-excel"
        if filename[-4:]==".csv":
            with tempfile.NamedTemporaryFile(suffix=".csv") as temp:
                run_extraction(assignee_IDs=disambiguated_assignee_IDs, output_path=temp.name, simplified=True)
                with open(temp.name, "rb") as file:
                    bytes_data = file.read()
            mime="text/csv"
    col3.download_button(label="Download", data=bytes_data, file_name=filename, mime=mime)