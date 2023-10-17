import streamlit as st
from er_evaluation.search import ElasticSearch
import pandas as pd
from sqlalchemy import create_engine, text
from openpyxl import Workbook
import tempfile
from assignee import disambiguated_assignees_data

DF_COLS = {
    "none": [],
    "elastic": [
        'assignee_organization',
        'assignee_individual_name_last',
        'assignee_individual_name_first',
        'assignee_country',
        'assignee_state',
        'assignee_city',
        'assignee_type',
        'assignee_id',
        '_score',
    ],
    "sql":[
        'organization',
        'name_last',
        'name_first',
        'assignee_country',
        'assignee_state',
        'assignee_city',
        'assignee_type',
        'disambiguated_assignee_id',
    ],
}


"""
## Disambiguated Assignee Search
"""

with st.expander("Information"):
    st.info("This is a demo search tool for disambiguated assignees. By default, the search is performed on the `assignees.assignee_organization` field, \
            aggregates by disambiguated assignee ID, and returns assignee information for the top hit within each aggregation bucket.")

    st.info("Aggregation searches can be time-consuming. Avoid including short keywords that may match a large number of companies (e.g., 'LLC' or 'Corp'). \
            If needed, increase the search timeout to up to a few minutes.")

def parse_csv(csv):
    return [x.strip() for x in csv.split(",")]

def parse_results(results, connection_type):
    if connection_type == "sql":
        return pd.DataFrame(results)
    elif connection_type == "elastic":
        agg_buckets = results["aggregations"]["assignees.assignee_id"]["assignees.assignee_id_inner"]["buckets"]
        df = pd.DataFrame.from_records(x["top_hits"]["hits"]["hits"][0]["_source"] for x in agg_buckets)
        df["_score"] = [x["top_hits"]["hits"]["hits"][0]["_score"] for x in agg_buckets]
        df.sort_values("_score", ascending=False, inplace=True)
        return df
    else:
        return pd.DataFrame()

with st.sidebar:

    with st.expander("SQL Connection", expanded=True):
        sql_host = st.text_input("Host", value="patentsview-ingest-production.cckzcdkkfzqo.us-east-1.rds.amazonaws.com")
        sql_user = st.text_input("User", value="sengineer")
        sql_pwd = st.text_input("Password", value="")
        db_name = st.text_input("DB Name", value="algorithms_assignee_labeling")

    with st.expander("ElasticSearch Connection", expanded=True):
        host = st.text_input("Host", value="https://patentsview-production-0cb426.es.us-east-1.aws.found.io")
        api_key = st.text_input("API Key", value="", help="API Key for authentication.")
    
    with st.expander("Configuration", expanded=True):
        timeout = st.number_input("Timeout", value=30, help="Search timeout in seconds.")
        index = st.text_input("Index", value="patents", help="Index to search in.")
        fuzziness = st.number_input("Fuzziness", value=2, help="Fuzziness level for matching.", min_value=0, max_value=2)
        col_select_placeholder = st.empty()

    with st.expander("Search Fields (comma separated):", expanded=False):
        source = parse_csv(st.text_input("Source", value="", help="Fields to return in the response."))
        agg_fields = parse_csv(st.text_input("Aggregation Fields", value="assignees.assignee_id", help="Fields to aggregate on."))
        agg_source = parse_csv(st.text_input("Aggregation Source", value="assignees", help="Fields to return for each top hit in the aggregations."))

# Establish connection
connection_type = "elastic" if api_key != "" else "sql" if sql_pwd != "" else "none"
es = ElasticSearch(host, api_key=api_key)
st.text(es.__dict__)
engine = create_engine(f"mysql+pymysql://{sql_user}:{sql_pwd}@{sql_host}/{db_name}?charset=utf8mb4")

# Search for user query
@st.cache_data
def search(user_query, index, fields, agg_fields, source, agg_source, timeout, size, fuzziness):
    if connection_type == "elastic":
        return es.search(user_query, index, fields, agg_fields=agg_fields, source=source, agg_source=agg_source, timeout=timeout, size=size, fuzziness=fuzziness)
    elif connection_type == "sql":
        query = f"SELECT * FROM algorithms_assignee_labeling.assignee WHERE {fields[0]}='{user_query}'"
        with engine.connect() as connection:
            results = connection.execute(text(query)).fetchall()
        return results

# Input query and processing
user_query = st.text_input("Search:", value="Lutron Electronics Co., Inc.", disabled=(api_key=="" and sql_pwd==""))
field_options = ["Organization", "First Name", "Last Name"]
field_select = st.radio("Fields:", field_options, horizontal=True, label_visibility="collapsed")
fields = [DF_COLS[connection_type][field_options.index(field_select)]] if connection_type != 'none' else None

# Execute search
with st.spinner('Searching...'):
    try:
        if connection_type == "none":
            st.write("**Please enter an API Key or a SQL connection.**")
            st.stop()
        else:
            results = search(user_query, index, fields, agg_fields=agg_fields, source=source, agg_source=agg_source, timeout=timeout, size=0, fuzziness=fuzziness)

    except Exception as e:
        st.error("Could not complete the search!", icon="🚨")
        st.error(e)
        st.stop()

    # Parse results into dataframe
    df = parse_results(results, connection_type)
    st.dataframe(df)
    cols = df.columns
    col_select = col_select_placeholder.multiselect("Columns to display:", options=cols, default=DF_COLS[connection_type])

    # Add editable column to indicate selection option
    df.insert(0, "Select", False)
    edited_df = st.data_editor(df[["Select"]+col_select])

    # Search statistics
    # entity_count = len(results["aggregations"]["assignees.assignee_id"]["assignees.assignee_id_inner"]["buckets"])
    # record_count = results["aggregations"]["assignees.assignee_id"]["doc_count"]
    # st.write(f"Found {entity_count} disambiguated assignees with {record_count} associated records.")

    # Selected data
    st.write("Selected Assignee IDs:")
    selected_df = df[edited_df["Select"] == True][[DF_COLS[connection_type][i] for i in [7, 0, 1, 2]]] # Get specific fields
    disambiguated_assignee_IDs = selected_df[DF_COLS[connection_type][7]].tolist()
    st.write(disambiguated_assignee_IDs)
    
if len(disambiguated_assignee_IDs) > 0:
    with st.spinner('Extracting...'):
        
        with engine.connect() as connection:
            mentions_table = disambiguated_assignees_data(disambiguated_assignee_IDs, connection)

        filename = st.text_input("Filename", "mentions_table.xlsx")
        with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
            mentions_table.save(temp.name)
            with open(temp.name, "rb") as file:
                bytes_data = file.read()
                st.download_button(label="Download", data=bytes_data, file_name=filename, mime="application/vnd.ms-excel")