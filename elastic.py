from dotenv import dotenv_values
from er_evaluation.search import ElasticSearch


def establish_connection():
    config = dotenv_values(".env")
    es_host = config['es_host']
    api_key = config['es_api_key']
    es = ElasticSearch(es_host, api_key=api_key)
    return es

user_query = "Lutron Electronics"
index = "assignee_references"
fields = ['assignee_organization']
agg_fields = ['assignee_id']
source = True
agg_source = ['']
timeout = 30
size = 0
fuzziness = 2


es = establish_connection()
results = es.search(user_query=user_query, index=index, fields=fields, agg_fields=agg_fields, source=source,\
          agg_source=agg_source, timeout=timeout, size=size, fuzziness=fuzziness)

print(results["aggregations"][f"{agg_fields[0]}_inner"]["buckets"][0]['top_hits']['hits']['hits'][0])