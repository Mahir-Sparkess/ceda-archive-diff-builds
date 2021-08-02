import argparse
from ceda_elasticsearch_tools.elasticsearch.ceda_elasticsearch_client import CEDAElasticsearchClient
from datetime import datetime

parser = argparse.ArgumentParser(description="Generate a list of dictionaries containing events")
parser.add_argument('source-index', help="name of source index")
parser.add_argument('dest-index', help="name of destination index")
args = parser.parse_args(["opensearch-collections-difftest", "opensearch-collections-difftest-dest"])

es = CEDAElasticsearchClient()


res = es.search(index=vars(args)['source-index'], body={"query": {"match_all": {}}})
source_hits = [hit['_source'] for hit in res['hits']['hits']]
source_set = set([hit['collection_id'] for hit in source_hits])

res = es.search(index=vars(args)['dest-index'], body={"query": {"match_all": {}}})
dest_hits = [hit['_source'] for hit in res['hits']['hits']]
dest_set = set([hit['collection_id'] for hit in dest_hits])

added = dest_set - source_set
removed = source_set - dest_set

events = []

for additions in added:
    hit = next(hit for hit in dest_hits if hit['collection_id'] == additions)
    events.append(
        {
            'collection_id' : hit['collection_id'],
            'collection_title' : hit['title'],
            'action' : 'added',
            'datetime' : datetime.now().isoformat(),

        }
    )

for removals in removed:
    hit = next(hit for hit in source_hits if hit['collection_id'] == removals)
    events.append(
        {
            'collection_id': hit['collection_id'],
            'collection_title': hit['title'],
            'action': 'removed',
            'datetime': datetime.now().isoformat(),

        }
    )

print(events)

