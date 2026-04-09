#!/usr/bin/env python3
"""Productive → Apollo sync.
Runs every minute. Pulls all open tasks assigned to team members that have dates,
creates/updates/removes Apollo entries in tasks.json.
Uses [from-productive] commit prefix so sync-to-productive does NOT re-trigger.
"""
import json, os, datetime, urllib.request, urllib.error

TOKEN = os.environ.get('PRODUCTIVE_TOKEN', 'c7e381f5-685e-4850-8bac-14941f97af46')
ORG   = '1476'
WIN   = datetime.date(2026, 3, 30)

HEADERS = {
    'X-Auth-Token': TOKEN,
    'X-Organization-Id': ORG,
    'Content-Type': 'application/vnd.api+json'
}

# Apollo ID → Productive person ID
PERSON_MAP = {
    '1': '1043514', '2': '456779',  '3': '934507',
    '4': '1066502', '5': '274241',  '6': '991921',
    '7': '990257',  '8': '777282'
}
# Reverse: Productive person ID → Apollo ID
PERSON_REVERSE = {v: k for k, v in PERSON_MAP.items()}

# Productive project name → Apollo proj key (partial match, case-insensitive)
PROJ_NAME_MAP = [
    ('early settler',   'essw'),
    ('jardan',          'jdb'),
    ('landsmith',       'lswb'),
    ('bywren',          'bsp'),
    ('by wren',         'bsp'),
    ('frank green',     'fgsp'),
    ('eckersley',       'eurd'),
    ('jag',             'jewd'),
    ('kidman',          'kgtr'),
    ('cable',           'cwm'),
    ('svdp',            'svdp'),
    ('replenishment',   'rf'),
    ('klaviyo',         'krpb'),
    ('lindelli',        'lisp'),
    ('space furniture', 'sfsp'),
    ('vinnies',         'vnnsp'),
    ('dapper',          'dpsp'),
    ('kelder',          'kldsp'),
    ('woolworths',      'wlsp'),
    ('baker',           'bksp'),
    ('mccormick',       'mcsp'),
]

def guess_proj(name):
    n = (name or '').lower()
    for key, proj in PROJ_NAME_MAP:
        if key in n:
            return proj
    return 'g'

def prod(path):
    req = urllib.request.Request(f'https://api.productive.io/api/v2{path}', headers=HEADERS)
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f'  Productive API {path[:60]}: {e.code} {e.read().decode()[:200]}')
        return None

def date_to_offset(d_str):
    if not d_str: return None
    d = datetime.date.fromisoformat(d_str)
    return (d - WIN).days

def workdays(s, e):
    count, d = 0, s
    while d <= e:
        if d.weekday() < 5: count += 1
        d += datetime.timedelta(days=1)
    return max(1, count)

def safe_hpd(apollo_pid):
    m = {'1':4.26,'2':5.78,'3':5.22,'4':4.90,'5':4.98,'6':3.96,'7':4.84,'8':6.00}
    return m.get(str(apollo_pid), 6.0)

# ── Load current tasks.json ──
with open('tasks.json') as f:
    tasks_data = json.load(f)

entries = tasks_data.get('entries', [])

# Index existing entries by prodTaskId for fast lookup
existing_by_task_id = {
    e['prodTaskId']: e for e in entries if e.get('prodTaskId')
}

# ── Fetch all open tasks assigned to each team member ──
all_prod_tasks = {}  # prodTaskId → task data

team_prod_ids = list(PERSON_REVERSE.keys())  # all 8 people

print(f'Fetching tasks for {len(team_prod_ids)} team members...')

page = 1
while True:
    # Build multi-assignee filter
    filter_parts = '&'.join([f'filter[assignee_id][]={pid}' for pid in team_prod_ids])
    url = (f'/tasks?{filter_parts}'
           f'&filter[workflow_status_category_id][]=1'
           f'&filter[workflow_status_category_id][]=2'
           f'&page[size]=100&page[number]={page}'
           f'&include=assignees,project')
    data = prod(url)
    if not data or not data.get('data'):
        break
    for task in data['data']:
        all_prod_tasks[task['id']] = task
    total = data.get('meta', {}).get('total_count', 0)
    if len(all_prod_tasks) >= total:
        break
    page += 1

print(f'Found {len(all_prod_tasks)} open tasks from Productive')

# ── Build included project lookup ──
# (included by &include=project — gives project name for proj key guessing)
# Note: included resources come in data['included'] array
# Since we do separate calls per page, project info may not be in included.
# Fetch project names separately for tasks that have project relationships.
proj_name_cache = {}  # prod_project_id → name

def get_proj_name(proj_id):
    if proj_id in proj_name_cache:
        return proj_name_cache[proj_id]
    data = prod(f'/projects/{proj_id}')
    if data and data.get('data'):
        name = data['data']['attributes'].get('name', '')
        proj_name_cache[proj_id] = name
        return name
    return ''

# ── Process each Productive task ──
changed = False
now_iso = datetime.datetime.utcnow().isoformat() + 'Z'

# Track which prodTaskIds we've seen (for deletion detection)
seen_task_ids = set()

for task_id, task in all_prod_tasks.items():
    attrs = task['attributes']
    title = attrs.get('title', '')
    start_str = attrs.get('start_date')
    due_str   = attrs.get('due_date')

    # Find assignee (first assignee that's in our team)
    assignee_apollo_id = None
    rels = task.get('relationships', {})

    # Try assignee relationship
    assignee_data = rels.get('assignee', {}).get('data')
    if assignee_data and isinstance(assignee_data, dict):
        prod_pid = assignee_data.get('id')
        assignee_apollo_id = PERSON_REVERSE.get(str(prod_pid))

    if not assignee_apollo_id:
        continue  # Can't map to Apollo person, skip

    # Skip tasks with no dates — they go to queue only
    if not start_str and not due_str:
        continue

    # Use whichever date we have
    effective_start = start_str or due_str
    effective_end   = due_str   or start_str

    s_off  = date_to_offset(effective_start)
    e_off  = date_to_offset(effective_end)
    if s_off is None:
        continue

    dn = max(1, e_off - s_off + 1) if e_off is not None else 1

    # Calculate hours from initial_estimate or safe hours
    initial_est_mins = attrs.get('initial_estimate') or 0
    if initial_est_mins:
        hrs = round(initial_est_mins / 60, 2)
    else:
        sd = WIN + datetime.timedelta(days=s_off)
        ed = WIN + datetime.timedelta(days=s_off + dn - 1)
        wd = workdays(sd, ed)
        hrs = round(wd * safe_hpd(assignee_apollo_id), 2)

    # Guess Apollo project from Productive project
    prod_proj_rel = rels.get('project', {}).get('data', {})
    prod_proj_id  = prod_proj_rel.get('id') if prod_proj_rel else None
    proj_name     = get_proj_name(prod_proj_id) if prod_proj_id else ''
    apollo_proj   = guess_proj(proj_name)

    # Build Productive task URL
    prod_task_url = f'https://app.productive.io/1476-dotcollective/tasks/{task_id}'

    seen_task_ids.add(task_id)

    if task_id in existing_by_task_id:
        # Update existing entry if Productive data has changed
        entry = existing_by_task_id[task_id]

        # Don't overwrite Apollo-originated changes that are newer
        # (Apollo changes have source='apollo', Productive pulls use 'productive')
        if entry.get('source') == 'apollo':
            # Apollo owns this — skip Productive update
            continue

        updates = {}
        if entry.get('s') != s_off:       updates['s']    = s_off
        if entry.get('dn') != dn:          updates['dn']   = dn; updates['n'] = dn
        if entry.get('note') != title:     updates['note'] = title; updates['title'] = title
        if entry.get('p') != assignee_apollo_id: updates['p'] = assignee_apollo_id
        if entry.get('proj') != apollo_proj and apollo_proj != 'g': updates['proj'] = apollo_proj

        if updates:
            entry.update(updates)
            entry['lastProdUpdate'] = now_iso
            changed = True
            print(f'  Updated: {title[:50]} → {updates}')
    else:
        # New task from Productive — add to Apollo
        import uuid
        new_entry = {
            'id':            str(uuid.uuid4()),
            'p':             assignee_apollo_id,
            'proj':          apollo_proj,
            'type':          None,
            's':             s_off,
            'n':             dn,
            'dn':            dn,
            'hrs':           hrs,
            'note':          title,
            'title':         title,
            'isLeave':       False,
            'prodTaskId':    task_id,
            'prodTaskUrl':   prod_task_url,
            'prodBookingId': None,
            'prodUrl':       None,
            'source':        'productive',
            'lastProdUpdate': now_iso
        }
        entries.append(new_entry)
        existing_by_task_id[task_id] = new_entry
        changed = True
        print(f'  Added: {title[:50]} ({effective_start} → {effective_end})')

# ── Remove entries that came FROM Productive but no longer exist ──
to_remove = []
for entry in entries:
    if entry.get('source') == 'productive' and entry.get('prodTaskId'):
        if entry['prodTaskId'] not in seen_task_ids:
            to_remove.append(entry)
            print(f'  Removed (task gone): {entry.get("note","?")[:50]}')
            changed = True

for entry in to_remove:
    entries.remove(entry)

# ── Save if anything changed ──
if changed:
    tasks_data['entries']   = entries
    tasks_data['generated'] = now_iso
    with open('tasks.json', 'w') as f:
        json.dump(tasks_data, f, indent=2)
    print(f'tasks.json updated ({len(entries)} entries)')
else:
    print('No changes — tasks.json unchanged')
