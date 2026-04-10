#!/usr/bin/env python3
"""Fetch Hannah's open Productive tasks → queue-data.json.
Runs every minute via sync-queue.yml GitHub Action.
"""
import urllib.request, json, datetime, os

TOKEN = os.environ.get('PRODUCTIVE_TOKEN', 'c7e381f5-685e-4850-8bac-14941f97af46')
ORG = '1476'
HEADERS = {
    'X-Auth-Token': TOKEN,
    'X-Organization-Id': ORG,
    'Content-Type': 'application/vnd.api+json'
}

def prod(path):
    req = urllib.request.Request(f'https://api.productive.io/api/v2{path}', headers=HEADERS)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

all_tasks = []
page = 1
while True:
    url = (f'/tasks?filter[assignee_id][]=777282'
           f'&filter[workflow_status_category_id][]=1'
           f'&filter[workflow_status_category_id][]=2'
           f'&page[size]=100&page[number]={page}')
    data = prod(url)
    all_tasks.extend(data.get('data', []))
    meta = data.get('meta', {})
    total = meta.get('total_count', 0)
    if len(all_tasks) >= total or not data.get('data'):
        break
    page += 1

output = {
    '_generated': datetime.datetime.utcnow().isoformat() + 'Z',
    'data': all_tasks,
    'meta': {'total_count': len(all_tasks)}
}

with open('queue-data.json', 'w') as f:
    json.dump(output, f, indent=2)

print(f'Fetched {len(all_tasks)} tickets')
# Re-enabled after API fix
