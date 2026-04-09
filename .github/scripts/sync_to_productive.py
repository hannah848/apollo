#!/usr/bin/env python3
"""Apollo → Productive booking sync.
Runs when tasks.json changes. Creates new bookings, updates changed ones, deletes removed ones.
"""
import json, os, subprocess, datetime, urllib.request, urllib.error

PROD_TOKEN = os.environ['PRODUCTIVE_TOKEN']
ORG = '1476'
WIN = datetime.date(2026, 3, 30)
FALLBACK_SERVICE = '8931117'

PERSON_MAP = {
    '1': '1043514', '2': '456779',  '3': '934507',
    '4': '1066502', '5': '274241',  '6': '991921',
    '7': '990257',  '8': '777282'
}
SAFE_HPD = {
    '1': 4.26, '2': 5.78, '3': 5.22, '4': 4.90,
    '5': 4.98, '6': 3.96, '7': 4.84, '8': 6.00
}

def offset_to_date(s):
    return (WIN + datetime.timedelta(days=int(s))).strftime('%Y-%m-%d')

def workdays(s, e):
    count, d = 0, datetime.datetime.strptime(s, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(e, '%Y-%m-%d').date()
    while d <= end:
        if d.weekday() < 5: count += 1
        d += datetime.timedelta(days=1)
    return count

def prod_req(method, path, body=None):
    url = f'https://api.productive.io/api/v2{path}'
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        'Content-Type': 'application/vnd.api+json',
        'X-Auth-Token': PROD_TOKEN,
        'X-Organization-Id': ORG,
        'User-Agent': 'apollo-sync'
    })
    try:
        with urllib.request.urlopen(req) as r:
            return {'ok': True} if r.status == 204 else json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f'  [{method}] {path} → {e.code}: {e.read().decode()[:200]}')
        return None

def calc_mins(entry):
    start = offset_to_date(entry['s'])
    dn = entry.get('dn') or entry.get('n') or 1
    end = offset_to_date(entry['s'] + dn - 1)
    wd = workdays(start, end)
    if entry.get('hrs'):
        return round(float(entry['hrs']) * 60)
    return round(wd * SAFE_HPD.get(str(entry.get('p', '1')), 6.0) * 60)

def make_payload(entry, prod_id=None):
    pid = PERSON_MAP.get(str(entry.get('p', '')))
    if not pid: return None
    start = offset_to_date(entry['s'])
    dn = entry.get('dn') or entry.get('n') or 1
    end = offset_to_date(entry['s'] + dn - 1)
    d = {
        'type': 'bookings',
        'attributes': {
            'started_on': start, 'ended_on': end,
            'total_time': calc_mins(entry),
            'booking_method_id': 3,
            'note': (entry.get('note') or entry.get('title') or '')[:255]
        },
        'relationships': {
            'person': {'data': {'type': 'people', 'id': pid}},
            'service': {'data': {'type': 'services', 'id': FALLBACK_SERVICE}}
        }
    }
    if prod_id: d['id'] = prod_id
    return {'data': d}

# Load current tasks.json
with open('tasks.json') as f:
    data = json.load(f)

# Load previous tasks.json from git history
try:
    result = subprocess.run(['git', 'show', 'HEAD^:tasks.json'],
                            capture_output=True, text=True, timeout=10)
    prev = json.loads(result.stdout) if result.returncode == 0 else {'entries': []}
except Exception as e:
    print(f'Could not load previous tasks.json: {e}')
    prev = {'entries': []}

prev_by_pid = {e['prodBookingId']: e for e in prev.get('entries', []) if e.get('prodBookingId')}
curr_pids = {e['prodBookingId'] for e in data.get('entries', []) if e.get('prodBookingId')}

# --- DELETE removed bookings ---
deleted_pids = set(prev_by_pid.keys()) - curr_pids
for pid in deleted_pids:
    note = prev_by_pid[pid].get('note', '?')[:40]
    print(f'DELETE {pid}: {note}')
    prod_req('DELETE', f'/bookings/{pid}')

# --- CREATE / UPDATE ---
changed = False
for entry in data.get('entries', []):
    if entry.get('isLeave') or entry.get('proj') == 'lv':
        continue
    prod_id = entry.get('prodBookingId')
    payload = make_payload(entry, prod_id)
    if not payload:
        continue

    if prod_id:
        # Only update if something relevant changed vs previous version
        prev_e = prev_by_pid.get(prod_id)
        if prev_e:
            same = (
                entry.get('s') == prev_e.get('s') and
                (entry.get('dn') or entry.get('n')) == (prev_e.get('dn') or prev_e.get('n')) and
                str(entry.get('p')) == str(prev_e.get('p')) and
                entry.get('hrs') == prev_e.get('hrs')
            )
            if same:
                continue  # No change, skip
        r = prod_req('PATCH', f'/bookings/{prod_id}', payload)
        status = '✓' if r else '✗'
        print(f'{status} UPDATE {prod_id}: {entry.get("note","")[:40]}')
    else:
        # Create new booking
        r = prod_req('POST', '/bookings', payload)
        if r and r.get('data'):
            new_id = r['data']['id']
            entry['prodBookingId'] = new_id
            changed = True
            print(f'✓ CREATE {new_id}: {entry.get("note","")[:40]}')
        else:
            print(f'✗ CREATE failed: {entry.get("note","")[:40]}')

if changed:
    data['_synced'] = datetime.datetime.utcnow().isoformat() + 'Z'
    with open('tasks.json', 'w') as f:
        json.dump(data, f, indent=2)
    total = sum(1 for e in data['entries'] if e.get('prodBookingId'))
    print(f'Wrote tasks.json — {total} entries with prodBookingId')
else:
    print('No prodBookingId changes — tasks.json unchanged')
