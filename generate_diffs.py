import argparse
import json
from ceda_elasticsearch_tools.elasticsearch.ceda_elasticsearch_client import CEDAElasticsearchClient
from datetime import datetime

parser = argparse.ArgumentParser(description="Generate a list of dictionaries containing events")
parser.add_argument("source-index", help="name of source index")
parser.add_argument("dest-index", help="name of destination index")
args = parser.parse_args()
es = CEDAElasticsearchClient()


def get_events(source_index: str, dest_index: str) -> list[dict]:
    res = es.search(index=source_index, body={"query": {"match_all": {}}})
    source_hits = [hit["_source"] for hit in res["hits"]["hits"]]
    source_set = set([hit["collection_id"] for hit in source_hits])

    res = es.search(index=dest_index, body={"query": {"match_all": {}}})
    dest_hits = [hit["_source"] for hit in res["hits"]["hits"]]
    dest_set = set([hit["collection_id"] for hit in dest_hits])

    added = dest_set - source_set
    removed = source_set - dest_set

    events = []

    for additions in added:
        hit = next(hit for hit in dest_hits if hit["collection_id"] == additions)
        events.append(
            {
                "collection_id": hit["collection_id"],
                "collection_title": hit["title"],
                "action": "added",
                "datetime": datetime.now().isoformat(),

            }
        )

    for removals in removed:
        hit = next(hit for hit in source_hits if hit["collection_id"] == removals)
        events.append(
            {
                "collection_id": hit["collection_id"],
                "collection_title": hit["title"],
                "action": "removed",
                "datetime": datetime.now().isoformat(),

            }
        )
    
    print(json.dumps(events, indent=4))
    return json.dumps(events, indent=4)


get_events(vars(args)["source-index"], vars(args)["dest-index"])
