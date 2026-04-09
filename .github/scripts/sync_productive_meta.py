#!/usr/bin/env python3
"""Fetch Productive projects + task lists → productive-meta.json.
Runs hourly. Used by Apollo modal to populate project/task-list pickers.
Only includes non-archived projects (archived_at == null).
"""
import json, os, urllib.request, urllib.error, datetime

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

# ── Fetch ALL projects, filter in Python to exclude archived ──
all_projects = []
page = 1
print('Fetching projects...')
while True:
    data = prod(f'/projects?page[size]=100&page[number]={page}')
    if not data or not data.get('data'):
        break
    for proj in data['data']:
        attrs = proj['attributes']
        # Skip archived projects (archived_at is set) and templates
        if attrs.get('archived_at') or attrs.get('template'):
            continue
        all_projects.append({
            'id':   proj['id'],
            'name': attrs.get('name', ''),
        })
    total = data.get('meta', {}).get('total_count', 0)
    fetched = (page * 100)
    if fetched >= total or not data.get('data'):
        break
    page += 1

print(f'  Found {len(all_projects)} active (non-archived) projects')

# ── Fetch task lists only for active projects ──
active_proj_ids = {p['id'] for p in all_projects}
all_task_lists = []
proj_ids = list(active_proj_ids)

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
            pid = proj_rel.get('id', '') if proj_rel else ''
            # Double-check: only include if project is active
            if pid not in active_proj_ids:
                continue
            all_task_lists.append({
                'id':        tl['id'],
                'name':      attrs.get('name', ''),
                'projectId': pid,
            })
        if not data.get('data') or len(data['data']) < 100:
            break
        page += 1

print(f'  Found {len(all_task_lists)} task lists')

# ── Write output ──
output = {
    '_generated': datetime.datetime.utcnow().isoformat() + 'Z',
    'projects':   all_projects,
    'taskLists':  all_task_lists,
}

with open('productive-meta.json', 'w') as f:
    json.dump(output, f, indent=2)

print(f'productive-meta.json written ({len(all_projects)} active projects, {len(all_task_lists)} task lists)')
