#!/usr/bin/env python3
"""Fetch Productive projects + task lists → productive-meta.json.
Runs hourly. Used by Apollo modal to populate project/task-list pickers.
"""
import json, os, urllib.request, urllib.error

TOKEN = os.environ.get('PRODUCTIVE_TOKEN', 'c7e381f5-685e-4850-8bac-14941f97af46')
ORG   = '1476'
HEADERS = {
    'X-Auth-Token': TOKEN,
    'X-Organization-Id': ORG,
    'Content-Type': 'application/vnd.api+json'
}

def prod(path):
    req = urllib.request.Request(
        f'https://api.productive.io/api/v2{path}', headers=HEADERS)
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f'  API {path[:60]}: {e.code} {e.read().decode()[:200]}')
        return None

# ── Fetch active projects ──
all_projects = []
page = 1
print('Fetching projects...')
while True:
    data = prod(f'/projects?filter[project_status_id][]=1&filter[project_status_id][]=2'
                f'&page[size]=100&page[number]={page}')
    if not data or not data.get('data'):
        break
    for proj in data['data']:
        attrs = proj['attributes']
        all_projects.append({
            'id':   proj['id'],
            'name': attrs.get('name', ''),
        })
    total = data.get('meta', {}).get('total_count', 0)
    if len(all_projects) >= total:
        break
    page += 1

print(f'  Found {len(all_projects)} projects')

# ── Fetch task lists for each project ──
# Batch by project ID — fetch all task lists, filter by active projects
all_task_lists = []
proj_ids = [p['id'] for p in all_projects]

# Productive supports filter[project_id][] for task lists
# Fetch in batches of 20 to avoid URL length limits
BATCH = 20
print(f'Fetching task lists for {len(proj_ids)} projects...')
for i in range(0, len(proj_ids), BATCH):
    batch = proj_ids[i:i+BATCH]
    filter_str = '&'.join([f'filter[project_id][]={pid}' for pid in batch])
    page = 1
    while True:
        data = prod(f'/task_lists?{filter_str}&page[size]=100&page[number]={page}')
        if not data or not data.get('data'):
            break
        for tl in data['data']:
            attrs = tl['attributes']
            proj_rel = tl.get('relationships', {}).get('project', {}).get('data', {})
            all_task_lists.append({
                'id':        tl['id'],
                'name':      attrs.get('name', ''),
                'projectId': proj_rel.get('id', '') if proj_rel else '',
            })
        total = data.get('meta', {}).get('total_count', 0)
        # Count for this batch only — use data length check
        if not data.get('data') or len(data['data']) < 100:
            break
        page += 1

print(f'  Found {len(all_task_lists)} task lists')

# ── Write output ──
import datetime
output = {
    '_generated': datetime.datetime.utcnow().isoformat() + 'Z',
    'projects':   all_projects,
    'taskLists':  all_task_lists,
}

with open('productive-meta.json', 'w') as f:
    json.dump(output, f, indent=2)

print(f'productive-meta.json written ({len(all_projects)} projects, {len(all_task_lists)} task lists)')
