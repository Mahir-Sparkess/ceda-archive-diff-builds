import argparse
import json
import requests
from ceda_elasticsearch_tools.elasticsearch.ceda_elasticsearch_client import CEDAElasticsearchClient
from datetime import datetime

parser = argparse.ArgumentParser(description="Generate a list of dictionaries containing events")
parser.add_argument("source-index", help="name of source index")
parser.add_argument("dest-index", help="name of destination index")
parser.add_argument("url", help="URL of the request location", default="localhost:8000/api/events/")
parser.add_argument("token", help="add authentication token")
args = parser.parse_args()
es = CEDAElasticsearchClient()
headers = {
    'Authorization': f'Token {args.token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}
url = args.url


def get_events(source_index: str, dest_index: str):
    """
    extracts and identifies changes between to indices, and outputs a collection of events.
    :param source_index: STRING, index of source
    :param dest_index: STRING index of destination
    :return: POST request of collection of events
    """

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

    # creation of record of events.
    events = []

    for additions in added:
        res = es.search(index=dest_index, body={"query": {"term": {'_id': additions}}})
        hit = res['hits']['hits'][0]['_source']
        events.append(
            {
                "collection_id": hit["collection_id"],
                "collection_title": hit["title"],
                "action": "added",
                "datetime": datetime.now().isoformat(),

            }
        )

    for removals in removed:
        res = es.search(index=source_index, body={"query": {"term": {'_id': removals}}})
        hit = res['hits']['hits'][0]['_source']
        events.append(
            {
                "collection_id": hit["collection_id"],
                "collection_title": hit["title"],
                "action": "removed",
                "datetime": datetime.now().isoformat(),

            }
        )

    for update in updated:
        res = es.search(index=dest_index, body={"query": {"term": {'drsId': update}}})
        collection_id = res['hits']['hits'][0]['_id']
        res = es.search(index=source_index, body={"query": {"term": {'_id': collection_id}}})
        hit = res['hits']['hits'][0]['_source']
        if hit:
            events.append(
                {
                    "collection_id": hit["collection_id"],
                    "collection_title": hit["title"],
                    "action": "updated",
                    "datetime": datetime.now().isoformat(),

                }
            )

    events_json = json.dumps(events, indent=4)
    # print(events_json)
    r = requests.post(
        url=url,
        data=events_json,
        headers=headers,
    )
    print(f'HTTP Response {r.status_code}')    # HTTP response


get_events(vars(args)["source-index"], vars(args)["dest-index"])
