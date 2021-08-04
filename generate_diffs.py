import argparse
import json
import requests
# import os
# from dotenv import load_dotenv
from ceda_elasticsearch_tools.elasticsearch.ceda_elasticsearch_client import CEDAElasticsearchClient
from datetime import datetime

parser = argparse.ArgumentParser(description="Generate a list of dictionaries containing events")
parser.add_argument("source-index", help="name of source index")
parser.add_argument("dest-index", help="name of destination index")
parser.add_argument("token", help="add authentication token")
args = parser.parse_args()
es = CEDAElasticsearchClient()
headers = {
    'Authorization': f'Token {args.token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}


def get_events(source_index, dest_index):
    res = es.search(index=source_index, body={"query": {"match_all": {}}})
    source_hits = [hit["_source"] for hit in res["hits"]["hits"]]
    source_set = set([hit["collection_id"] for hit in source_hits])
    source_drs_set = set([tuple(hit['drsId']) for hit in source_hits if hit.get('drsId')])

    res = es.search(index=dest_index, body={"query": {"match_all": {}}})
    dest_hits = [hit["_source"] for hit in res["hits"]["hits"]]
    dest_set = set([hit["collection_id"] for hit in dest_hits])
    dest_drs_set = set([tuple(hit['drsId']) for hit in source_hits if hit.get('drsId')])

    added = dest_set - source_set
    removed = source_set - dest_set
    updated = dest_drs_set - source_drs_set

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

    for update in updated:
        update_collection_id = next(hit["collection_id"] for hit in dest_hits if hit["drsId"] == update)
        hit = next(hit for hit in source_hits if hit["collection_id"] == update_collection_id)
        events.append(
            {
                "collection_id": hit["collection_id"],
                "collection_title": hit["title"],
                "action": "updated",
                "datetime": datetime.now().isoformat(),

            }
        )

    # print(requests.get("http://127.0.0.1:8000/api/events/").json())
    events_json = json.dumps(events, indent=4)
    print(events_json)
    '''r = requests.post(
        "http://127.0.0.1:8000/api/events/",
        data=events_json,
        headers=headers,
    )
    print(r.status_code)'''



get_events(vars(args)["source-index"], vars(args)["dest-index"])
