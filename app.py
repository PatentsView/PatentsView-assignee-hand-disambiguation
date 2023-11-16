import streamlit as st
from er_evaluation.search import ElasticSearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from extraction import run_extraction, simple_extraction_output, convert_assignee_type
import pandas as pd
import tempfile
from dotenv import dotenv_values

PORT = "443"
DF_COLS = {
    "none": [],
    "elastic": {
        'assignee_organization': 'assignees.assignee_organization',
        'assignee_individual_name_first': 'assignees.assignee_individual_name_first',
        'assignee_individual_name_last': 'assignees.assignee_individual_name_last',
        'assignee_country': 'assignees.assignee_country',
        'assignee_state': 'assignees.assignee_state',
        'assignee_city': 'assignees.assignee_city',
        'assignee_type': 'assignees.assignee_type',
        'assignee_id': 'assignees.assignee_id',
        'assignee_reference_id': 'assignees.assignee_reference_id',
        # '_score': '_score',
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
    es = Elasticsearch(hosts=f"{config['es_host']}:{PORT}", api_key=config['es_api_key'], request_timeout=timeout)
    # es = ElasticSearch(config['es_host'], api_key=config['es_api_key'])
    return es

def parse_csv(csv):
    return [x.strip() for x in csv.split(",")]

def parse_results(results):
    df = pd.DataFrame(results)
    df['assignee_type'] = df['assignee_type'].apply(lambda x: convert_assignee_type(x))
    df = df[['assignee_organization', 'assignee_id', 'assignee_individual_name_first', 'assignee_individual_name_last',\
            'assignee_type', 'assignee_city', 'assignee_state', 'assignee_country', 'assignee_reference_id']]
    return df

def _create_nested_query(field, full_field_path, query):
    """Helper function to create nested queries"""
    path = field.split(".")
    if len(path) > 1:
        return {
            "nested": {
                "path": path[0],
                "query": _create_nested_query(".".join(path[1:]), full_field_path, query),
            }
        }
    else:
        return {"match": {full_field_path: query}}

def process_query(user_query, fields, fuzziness=2):
    """
    Process the user's query and build a search query for Elasticsearch.

    Args:
        user_query: The user's query string.
        fields: A list of fields to search in.
        fuzziness: The fuzziness level for matching (optional, default: 2).
    Returns:
        A dictionary representing the Elasticsearch search query.
    """

    must_clauses = []
    for field in fields:
        query = {"query": user_query, "fuzziness": fuzziness}
        must_clauses.append(_create_nested_query(field, field, query))

    return {"bool": {"should": must_clauses}}

@st.cache_data
def search(user_query, index, fields, agg_fields, source, agg_source, timeout, size, fuzziness):
    s = Search(using=es, index=index)
    search_dict = {
        "aggregations": agg_fields,
        "size": size,
        "query": process_query(user_query, fields, fuzziness),
    }
    s.update_from_dict(search_dict)
    response = s.execute()
    return [hit.to_dict() for hit in response]

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
        fuzziness = st.number_input("Fuzziness", value=2, help="Fuzziness level for matching.", min_value=0, max_value=2)
        size = st.number_input("Size", value=50, help="Number of search results to display.", min_value=5, max_value=10000)
        col_select_placeholder = st.empty()

    with st.expander("Search Fields (comma separated):", expanded=False):
        source = parse_csv(st.text_input("Source", value="", help="Fields to return in the response.", )) # list(DF_COLS["elastic"].keys()) 
        agg_fields = parse_csv(st.text_input("Aggregation Fields", value="", help="Fields to aggregate on."))
        agg_source = parse_csv(st.text_input("Aggregation Source", value="", help="Fields to return for each top hit in the aggregations."))

"""
Mention ID:
"""
import json
import requests
from extraction import BASE_URL, API_KEY
@st.cache_data
def mention_id_data(mention_id):
    return simple_extraction_output(mention_id)

# Define mention ID variables
col1, col2, col3 = st.columns([3, 1, 1])
mention_id = col1.text_input(label="Mention ID", placeholder="Paste Mention ID Here", value="", label_visibility="collapsed")
patent_id = mention_id.split("-")[0]

# Check for user input
if len(mention_id) > 0:
    pv_url = f"https://datatool.patentsview.org/#detail/patent/{patent_id[2:]}/"
    gp_url = f"https://patents.google.com/patent/{patent_id}/"
    try:
        assignee_mention_data = mention_id_data(mention_id)
        st.dataframe(assignee_mention_data)
    except Exception as e:
        st.error("MySQL may be down. Please use PatentsView or Google Patents instead.", icon="🚨")
        st.error(e)
else:
    pv_url = "https://datatool.patentsview.org/#search&pat=2|"
    gp_url = "https://patents.google.com/"
    assignee_mention_data = None

# Create links to PV and Google Patents
col2.link_button(label="PatentsView", url=pv_url)
col3.link_button(label="Google Patents", url=gp_url)


"""
Search:
"""
es = establish_connection()
user_query_null = "" if assignee_mention_data is None else assignee_mention_data['value'][9]
user_query = st.text_input(label="Search", value=user_query_null, label_visibility="collapsed")
field_options = ["Organization", "First Name", "Last Name"]
field_select = st.radio("Fields:", field_options, horizontal=True, label_visibility="collapsed")
fields = [list(DF_COLS["elastic"].keys())[field_options.index(field_select)]]

# Execute search
if len(user_query) > 0:
    try:
        results = search(user_query=user_query, index=index, fields=fields, agg_fields=[],\
                        source=[], agg_source=[], timeout=timeout, size=size, fuzziness=fuzziness)
    except Exception as e:
        st.error("Could not complete the search!", icon="🚨")
        st.error(e)
        st.stop()

    # Parse results into dataframe
    search_df = parse_results(results)
    cols = [i for i in search_df.columns]
    col_select = col_select_placeholder.multiselect("Columns to display:", options=cols, default=cols)
    if 'selection_rows' not in st.session_state: # Record all search results and user selections
        st.session_state.selection_rows = []
        st.session_state.selected_assignee_ids = set()

    # Generates editable table with select column reflecting st.session_state
    search_df.insert(0, "Select", False)
    @st.cache_data
    def generate_table(user_query, selected_assignee_ids, col_select, size):
        search_df["Select"] = search_df["assignee_id"].isin(selected_assignee_ids)
        return search_df[["Select"]+col_select]

    """
    SEARCH DataFrame:
    """
    edited_df = st.data_editor(generate_table(user_query, st.session_state.selected_assignee_ids, col_select, size))

    # Button row
    col1, col2, col3, col4 = st.columns([1.5, 2, 2, 2.5])

    """
    SELECTION DataFrame:
    """    
    if len(st.session_state.selection_rows) > 0:
        selection_df = pd.DataFrame(st.session_state.selection_rows)
        selection_df.insert(0, "Remove", False)
        edited_selection_df = st.data_editor(selection_df[["Remove"]+col_select])
    
    # Add all rows selected in SEARCH to the SELECTIONS df
    if col1.button("Add from SEARCH"):
        # Get all selected rows and update selected_assignee_ids set
        addition_ids = search_df[edited_df["Select"]]["assignee_id"].tolist()
        st.session_state.selected_assignee_ids.update(addition_ids)

        # Add all rows with assignee_id in selected_assignee_ids to selection_rows
        prior_reference_ids = [row['assignee_reference_id'] for row in st.session_state.selection_rows]
        for index, data in search_df.iterrows():
            if data['assignee_reference_id'] not in prior_reference_ids\
                and data['assignee_id'] in st.session_state.selected_assignee_ids:
                st.session_state.selection_rows.append(data)

    # Remove all rows selected from SELECTIONS df and unselect in SEARCH df
    if col2.button("Remove from SELECTIONS"):
        removal_ids = selection_df[edited_selection_df["Remove"]]['assignee_id'].tolist()
        st.session_state.selected_assignee_ids -= set(removal_ids)
        st.session_state.selection_rows = [row for row in st.session_state.selection_rows if row['assignee_id'] not in removal_ids]

    # Add all rows from the SEARCH df to the SELECTIONS df
    if col3.button("Add ALL from SEARCH"):
        prior_reference_ids = [row['assignee_reference_id'] for row in st.session_state.selection_rows]
        for index, data in search_df.iterrows():
            st.session_state.selected_assignee_ids.add(data['assignee_id'])
            if data['assignee_reference_id'] not in prior_reference_ids:
                st.session_state.selection_rows.append(data)

    # Remove all rows from the SELECTIONS df and deselect all from SEARCH df
    if col4.button("Remove ALL from SELECTIONS"):
        st.session_state.selection_rows = []
        st.session_state.selected_assignee_ids = set()

    """
    Extraction and Download:
    """
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    filename_value = (mention_id+".csv") if len(mention_id) > 0 else ""
    filename = col1.text_input(label="Filename", placeholder="Filename", value=filename_value, label_visibility="collapsed")

    # Create the bytes data file necessary for downloading output
    def extract_output(simplified, filename):
        if filename[-4:]==".csv":
            with tempfile.NamedTemporaryFile(suffix=".csv") as temp:
                run_extraction(assignee_IDs=list(st.session_state.selected_assignee_ids), output_path=temp.name, simplified=simplified)
                with open(temp.name, "rb") as file:
                    bytes_data = file.read()
            mime="text/csv"
        elif filename[-5:]==".xlsx":
            with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
                run_extraction(assignee_IDs=list(st.session_state.selected_assignee_ids), output_path=temp.name, simplified=simplified)
                with open(temp.name, "rb") as file:
                    bytes_data = file.read()
            mime="application/vnd.ms-excel"
        return bytes_data, mime

    # Download regular output
    if col2.button("Extract simplified"):
        with st.spinner('Extracting...'):
            bytes_data, mime = extract_output(simplified=True, filename=filename)
        col4.download_button(label="Download", data=bytes_data, file_name=filename, mime=mime)

    # Download output with inventor and CPC information
    if col3.button("Extract complex"):
        with st.spinner('Extracting...'):
            bytes_data, mime = extract_output(simplified=False, filename=filename)
        col4.download_button(label="Download", data=bytes_data, file_name=filename, mime=mime)