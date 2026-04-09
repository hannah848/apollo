#!/usr/bin/env python3
"""Apollo → Productive sync.
Runs when tasks.json changes (commits prefixed with 'Apollo:').
For each entry:
  - source='productive': skip (Productive owns these, sync-from-productive handles them)
  - source='apollo' (or no source): create/update Productive TASK + BOOKING
    Currently only for Hannah (person 8) — expand after testing
Deletions: if entry was in prev but not in curr → delete task + booking in Productive.

Loop prevention: commits from this script use '[from-productive]' prefix? No —
this uses 'sync:' prefix. sync-to-productive only fires on 'Apollo:' commits.
"""
import json, os, subprocess, datetime, urllib.request, urllib.error

PROD_TOKEN = os.environ.get('PRODUCTIVE_TOKEN', 'c7e381f5-685e-4850-8bac-14941f97af46')
ORG   = '1476'
WIN   = datetime.date(2026, 3, 30)

# Fallback service and task list for task creation when not specified
FALLBACK_SERVICE   = '8931117'  # Design service
FALLBACK_TASK_LIST = None       # Never create tasks without a known active project

# Only create Productive tasks for these Apollo person IDs (expand after testing)
TASK_CREATION_ALLOWED = {'8'}   # Hannah only during testing

PERSON_MAP = {
    '1': '1043514', '2': '456779',  '3': '934507',
    '4': '1066502', '5': '274241',  '6': '991921',
    '7': '990257',  '8': '777282'
}
SAFE_HPD = {
    '1': 4.26, '2': 5.78, '3': 5.22, '4': 4.90,
    '5': 4.98, '6': 3.96, '7': 4.84, '8': 6.00
}

# Apollo proj key → Productive project name fragment (for meta lookup)
PROJ_TO_NAME_HINT = {
    'essw': 'early settler', 'jdb': 'jardan',    'lswb': 'landsmith',
    'bsp':  'bywren',        'fgsp': 'frank green', 'eurd': 'eckersley',
    'jewd': 'jag',           'kgtr': 'kidman',    'cwm':  'cable',
    'svdp': 'svdp',          'rf':   'replenishment', 'krpb': 'klaviyo',
    'lisp': 'lindelli',      'sfsp': 'space furniture', 'vnnsp': 'vinnies',
    'dpsp': 'dapper',        'kldsp': 'kelder',   'wlsp': 'woolworths',
    'bksp': 'baker',         'mcsp': 'mccormick',
}

def offset_to_date(s):
    return (WIN + datetime.timedelta(days=int(s))).strftime('%Y-%m-%d')

def workdays(s_str, e_str):
    count, d = 0, datetime.date.fromisoformat(s_str)
    end = datetime.date.fromisoformat(e_str)
    while d <= end:
        if d.weekday() < 5: count += 1
        d += datetime.timedelta(days=1)
    return max(1, count)

def prod_req(method, path, body=None):
    url = f'https://api.productive.io/api/v2{path}'
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        'Content-Type': 'application/vnd.api+json',
        'X-Auth-Token': PROD_TOKEN,
        'X-Organization-Id': ORG,
        'User-Agent': 'apollo-sync/2.0'
    })
    try:
        with urllib.request.urlopen(req) as r:
            return {'ok': True, 'status': r.status} if r.status == 204 else json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()[:300]
        if e.code == 403 and method in ('PATCH', 'DELETE'):
            print(f'  [{method}] {path[:60]} → 403 (no write permission — skip)')
            return {'ok': False, 'status': 403, 'skipped': True}
        print(f'  [{method}] {path[:60]} → {e.code}: {body_text}')
        return None

def calc_mins(entry):
    start = offset_to_date(entry['s'])
    dn    = entry.get('dn') or entry.get('n') or 1
    end   = offset_to_date(entry['s'] + dn - 1)
    wd    = workdays(start, end)
    if entry.get('hrs'):
        return round(float(entry['hrs']) * 60)
    return round(wd * SAFE_HPD.get(str(entry.get('p', '1')), 6.0) * 60)

# ── Load productive-meta.json for project/task list lookup ──
meta_projects   = {}  # name_lower → id
meta_task_lists = {}  # project_id → list of {id, name}
try:
    with open('productive-meta.json') as f:
        meta = json.load(f)
    for p in meta.get('projects', []):
        # productive-meta.json already excludes archived — trust it
        meta_projects[p['name'].lower()] = p['id']
    for tl in meta.get('taskLists', []):
        pid = tl['projectId']
        meta_task_lists.setdefault(pid, []).append(tl)
    print(f'Loaded productive-meta.json: {len(meta_projects)} projects, {len(meta_task_lists)} project task-list groups')
except FileNotFoundError:
    print('productive-meta.json not found — project lookup disabled')

def find_prod_project_id(apollo_proj_key):
    """Try to find Productive project ID from Apollo project key."""
    hint = PROJ_TO_NAME_HINT.get(apollo_proj_key, '')
    if not hint:
        return None
    for name_lower, pid in meta_projects.items():
        if hint in name_lower:
            return pid
    return None

def find_task_list_id(prod_project_id, preferred_name='Scheduling'):
    """Find first task list in active project. Returns None if project not found or archived."""
    if not prod_project_id:
        return None  # Don't fall back to random task list — only use known active projects
    tls = meta_task_lists.get(prod_project_id, [])
    if not tls:
        return None
    # Prefer 'Scheduling' > 'Pending' > 'Tasks' > first
    for preferred in [preferred_name, 'Pending', 'Tasks', 'To Do']:
        for tl in tls:
            if preferred.lower() in tl['name'].lower():
                return tl['id']
    return tls[0]['id']

def make_task_payload(entry, prod_person_id, task_list_id, prod_proj_id=None):
    """Build Productive task creation payload."""
    title = entry.get('note') or entry.get('title') or 'Apollo booking'
    start = offset_to_date(entry['s'])
    dn    = entry.get('dn') or entry.get('n') or 1
    end   = offset_to_date(entry['s'] + dn - 1)
    rels = {
        'task_list': {'data': {'type': 'task_lists', 'id': task_list_id}},
        'assignees': {'data': [{'type': 'people',    'id': prod_person_id}]},
    }
    if prod_proj_id:
        rels['project'] = {'data': {'type': 'projects', 'id': prod_proj_id}}
    payload = {
        'data': {
            'type': 'tasks',
            'attributes': {
                'title':      title,
                'start_date': start,
                'due_date':   end,
            },
            'relationships': rels,
        }
    }
    return payload

def make_booking_payload(entry, prod_person_id, prod_task_id=None, booking_id=None):
    """Build Productive booking payload."""
    start = offset_to_date(entry['s'])
    dn    = entry.get('dn') or entry.get('n') or 1
    end   = offset_to_date(entry['s'] + dn - 1)
    d = {
        'type': 'bookings',
        'attributes': {
            'started_on':        start,
            'ended_on':          end,
            'total_time':        calc_mins(entry),
            'booking_method_id': 3,
            'note':              (entry.get('note') or entry.get('title') or '')[:255]
        },
        'relationships': {
            'person':  {'data': {'type': 'people',   'id': prod_person_id}},
            'service': {'data': {'type': 'services',  'id': FALLBACK_SERVICE}}
        }
    }
    if prod_task_id:
        d['relationships']['task'] = {'data': {'type': 'tasks', 'id': prod_task_id}}
    if booking_id:
        d['id'] = booking_id
    return {'data': d}

# ── Load current + previous tasks.json ──
with open('tasks.json') as f:
    data = json.load(f)

try:
    result = subprocess.run(['git', 'show', 'HEAD^:tasks.json'],
                            capture_output=True, text=True, timeout=10)
    prev = json.loads(result.stdout) if result.returncode == 0 else {'entries': []}
except Exception as e:
    print(f'Could not load previous tasks.json: {e}')
    prev = {'entries': []}

curr_entries = data.get('entries', [])
prev_entries = prev.get('entries', [])

# Index by entry ID for change detection
curr_by_id = {e['id']: e for e in curr_entries if e.get('id')}
prev_by_id = {e['id']: e for e in prev_entries if e.get('id')}

# Also index previous by prodBookingId for legacy deletion
prev_by_booking_id = {
    e['prodBookingId']: e for e in prev_entries
    if e.get('prodBookingId') and not e.get('prodTaskId')
}

changed = False

# ── DELETIONS: entries in prev but not in curr ──
print('Checking for deletions...')
for eid, prev_e in prev_by_id.items():
    if eid in curr_by_id:
        continue  # Still exists

    source = prev_e.get('source', 'apollo')
    if source == 'productive':
        # Productive-owned — deletion in Apollo triggered by user; delete from Productive too
        task_id    = prev_e.get('prodTaskId')
        booking_id = prev_e.get('prodBookingId')
        note = prev_e.get('note', '?')[:40]
        if booking_id:
            print(f'DELETE booking {booking_id}: {note}')
            prod_req('DELETE', f'/bookings/{booking_id}')
        if task_id:
            print(f'DELETE task {task_id}: {note}')
            prod_req('DELETE', f'/tasks/{task_id}')
        continue

    # Apollo-sourced deletion
    task_id    = prev_e.get('prodTaskId')
    booking_id = prev_e.get('prodBookingId')
    note = prev_e.get('note', '?')[:40]
    apollo_pid = str(prev_e.get('p', ''))
    if apollo_pid not in TASK_CREATION_ALLOWED:
        # Not in test scope — just delete booking if exists
        if booking_id:
            print(f'DELETE booking {booking_id}: {note}')
            prod_req('DELETE', f'/bookings/{booking_id}')
        continue
    if booking_id:
        print(f'DELETE booking {booking_id}: {note}')
        prod_req('DELETE', f'/bookings/{booking_id}')
    if task_id:
        print(f'DELETE task {task_id}: {note}')
        prod_req('DELETE', f'/tasks/{task_id}')

# ── Legacy: delete bookings that lost prodBookingId (pre-prodTaskId entries) ──
curr_booking_ids = {e['prodBookingId'] for e in curr_entries if e.get('prodBookingId')}
for bid, prev_e in prev_by_booking_id.items():
    if bid not in curr_booking_ids:
        note = prev_e.get('note', '?')[:40]
        print(f'DELETE legacy booking {bid}: {note}')
        prod_req('DELETE', f'/bookings/{bid}')

# ── CREATE / UPDATE ──
print('Processing creates/updates...')
for entry in curr_entries:
    if entry.get('isLeave') or entry.get('proj') == 'lv':
        continue

    source     = entry.get('source', 'apollo')
    apollo_pid = str(entry.get('p', ''))
    prod_pid   = PERSON_MAP.get(apollo_pid)
    if not prod_pid:
        continue

    # Productive-owned entries: only sync bookings, don't touch tasks
    if source == 'productive':
        prod_booking_id = entry.get('prodBookingId')
        prev_e = prev_by_id.get(entry.get('id', ''))
        if prod_booking_id and prev_e:
            same = (
                entry.get('s') == prev_e.get('s') and
                (entry.get('dn') or entry.get('n')) == (prev_e.get('dn') or prev_e.get('n')) and
                str(entry.get('p')) == str(prev_e.get('p')) and
                entry.get('hrs') == prev_e.get('hrs')
            )
            if same:
                continue
            payload = make_booking_payload(entry, prod_pid, entry.get('prodTaskId'), prod_booking_id)
            r = prod_req('PATCH', f'/bookings/{prod_booking_id}', payload)
            status = '✓' if r else '✗'
            print(f'{status} UPDATE booking {prod_booking_id}: {entry.get("note","")[:40]}')
        continue

    # Apollo-sourced entries
    prod_task_id    = entry.get('prodTaskId')
    prod_booking_id = entry.get('prodBookingId')
    prev_e = prev_by_id.get(entry.get('id', ''))

    # ── CREATE: new entry with no Productive IDs ──
    if not prod_task_id and not prod_booking_id:
        # Only create tasks for allowed persons (Hannah during testing)
        if apollo_pid in TASK_CREATION_ALLOWED:
            # Look up project + task list
            prod_proj_id  = find_prod_project_id(entry.get('proj', 'g'))
            task_list_id  = find_task_list_id(prod_proj_id)

            if task_list_id:
                task_payload = make_task_payload(entry, prod_pid, task_list_id, prod_proj_id)
                r = prod_req('POST', '/tasks', task_payload)
                if r and r.get('data'):
                    prod_task_id = r['data']['id']
                    entry['prodTaskId']  = prod_task_id
                    entry['prodTaskUrl'] = f'https://app.productive.io/1476-dotcollective/tasks/{prod_task_id}'
                    entry['source']      = 'apollo'
                    changed = True
                    print(f'✓ CREATE task {prod_task_id}: {entry.get("note","")[:40]}')
                else:
                    print(f'✗ CREATE task failed: {entry.get("note","")[:40]}')
            else:
                print(f'  No task list found for proj={entry.get("proj")} — skipping task creation')

        # Create booking (for all persons, not just Hannah)
        payload = make_booking_payload(entry, prod_pid, prod_task_id)
        r = prod_req('POST', '/bookings', payload)
        if r and r.get('data'):
            new_bid = r['data']['id']
            entry['prodBookingId'] = new_bid
            changed = True
            print(f'✓ CREATE booking {new_bid}: {entry.get("note","")[:40]}')
        else:
            print(f'✗ CREATE booking failed: {entry.get("note","")[:40]}')
        continue

    # ── UPDATE: check if anything changed ──
    if not prev_e:
        continue  # Can't compare, skip

    date_changed = (
        entry.get('s') != prev_e.get('s') or
        (entry.get('dn') or entry.get('n')) != (prev_e.get('dn') or prev_e.get('n'))
    )
    person_changed  = str(entry.get('p')) != str(prev_e.get('p'))
    hours_changed   = entry.get('hrs') != prev_e.get('hrs')
    note_changed    = entry.get('note') != prev_e.get('note')

    if not (date_changed or person_changed or hours_changed or note_changed):
        continue  # Nothing changed

    # Update Productive task dates/title if we own it
    if prod_task_id and apollo_pid in TASK_CREATION_ALLOWED:
        if date_changed or note_changed:
            start = offset_to_date(entry['s'])
            dn    = entry.get('dn') or entry.get('n') or 1
            end   = offset_to_date(entry['s'] + dn - 1)
            task_update = {
                'data': {
                    'id': prod_task_id,
                    'type': 'tasks',
                    'attributes': {
                        'title':      entry.get('note') or entry.get('title') or 'Apollo booking',
                        'start_date': start,
                        'due_date':   end,
                    }
                }
            }
            if person_changed:
                task_update['data']['relationships'] = {
                    'assignee': {'data': {'type': 'people', 'id': prod_pid}}
                }
            r = prod_req('PATCH', f'/tasks/{prod_task_id}', task_update)
            status = '✓' if r else '✗'
            print(f'{status} UPDATE task {prod_task_id}: {entry.get("note","")[:40]}')

    # Update booking
    if prod_booking_id:
        payload = make_booking_payload(entry, prod_pid, prod_task_id, prod_booking_id)
        r = prod_req('PATCH', f'/bookings/{prod_booking_id}', payload)
        status = '✓' if r else '✗'
        print(f'{status} UPDATE booking {prod_booking_id}: {entry.get("note","")[:40]}')
    elif prod_task_id:
        # Task exists but no booking yet — create booking
        payload = make_booking_payload(entry, prod_pid, prod_task_id)
        r = prod_req('POST', '/bookings', payload)
        if r and r.get('data'):
            entry['prodBookingId'] = r['data']['id']
            changed = True
            print(f'✓ CREATE booking for existing task {prod_task_id}: {entry.get("note","")[:40]}')

# ── Save updated prodTaskIds/prodBookingIds back to tasks.json ──
if changed:
    data['entries']  = curr_entries
    data['_synced']  = datetime.datetime.utcnow().isoformat() + 'Z'
    with open('tasks.json', 'w') as f:
        json.dump(data, f, indent=2)
    n_tasks    = sum(1 for e in curr_entries if e.get('prodTaskId'))
    n_bookings = sum(1 for e in curr_entries if e.get('prodBookingId'))
    print(f'tasks.json updated — {n_tasks} prodTaskIds, {n_bookings} prodBookingIds')
else:
    print('No changes to write back.')
