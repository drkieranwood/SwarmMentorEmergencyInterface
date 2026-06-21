#!/usr/bin/env python3
"""
parse_logs.py — split a ULog flight log into per-drone CSV histories.

Outputs two CSVs per drone:
  drone_{id}_telem.csv  — all telemetry rows (lat, lon, alt, heading, batt, ...)
  drone_{id}_events.csv — CONNECT/DISCONNECT events and commands

Usage:
    python parse_logs.py logs/20240101_120000.log
    python parse_logs.py logs/20240101_120000.log --out my_output_dir
    python parse_logs.py --latest          # picks newest .log in logs/
"""

import argparse
import csv
import os
import sys
from collections import defaultdict


def _parse_kv(tokens):
    d = {}
    for t in tokens:
        if '=' in t:
            k, _, v = t.partition('=')
            d[k] = v
    return d


def parse_log(filepath):
    telem  = defaultdict(list)   # drone_id -> [{...}, ...]
    events = defaultdict(list)

    with open(filepath, encoding='utf-8') as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split()
            if len(parts) < 3:
                continue

            ts, record_type, *rest = parts
            kv = _parse_kv(rest)
            drone_id = kv.get('id', 'unknown')

            if record_type == 'TELEM':
                telem[drone_id].append({
                    'timestamp': ts,
                    'lat':      kv.get('lat',      ''),
                    'lon':      kv.get('lon',      ''),
                    'alt':      kv.get('alt',      ''),
                    'heading':  kv.get('hdg',      ''),
                    'batt':     kv.get('batt',     ''),
                    'speed_mps': kv.get('spd',     ''),
                    'mode':     kv.get('mode',     ''),
                    'armed':    kv.get('armed',    ''),
                    'airborne': kv.get('airborne', ''),
                })

            elif record_type == 'EVENT':
                events[drone_id].append({
                    'timestamp': ts,
                    'type':      'EVENT',
                    'event':     kv.get('event', ''),
                    'action':    '',
                    'detail':    '',
                })

            elif record_type == 'CMD':
                detail_tokens = [t for t in rest if not t.startswith('id=') and not t.startswith('action=')]
                events[drone_id].append({
                    'timestamp': ts,
                    'type':      'CMD',
                    'event':     '',
                    'action':    kv.get('action', ''),
                    'detail':    ' '.join(detail_tokens),
                })

    return telem, events


def write_csvs(telem, events, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    all_ids = sorted(set(telem) | set(events), key=lambda x: str(x).zfill(10))

    for drone_id in all_ids:
        rows = telem.get(drone_id, [])
        if rows:
            path = os.path.join(out_dir, f'drone_{drone_id}_telem.csv')
            with open(path, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=['timestamp', 'lat', 'lon', 'alt', 'heading', 'batt', 'speed_mps', 'mode', 'armed', 'airborne'])
                w.writeheader()
                w.writerows(rows)
            print(f"  [{drone_id}] {len(rows):>6} telemetry rows  -> {path}")

        rows = events.get(drone_id, [])
        if rows:
            path = os.path.join(out_dir, f'drone_{drone_id}_events.csv')
            with open(path, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=['timestamp', 'type', 'event', 'action', 'detail'])
                w.writeheader()
                w.writerows(rows)
            print(f"  [{drone_id}] {len(rows):>6} event rows      -> {path}")


def main():
    ap = argparse.ArgumentParser(description="Split a ULog flight log into per-drone CSVs.")
    ap.add_argument('logfile', nargs='?', help='Path to .log file')
    ap.add_argument('--latest', action='store_true', help='Use the newest .log file in logs/')
    ap.add_argument('--out', default=None, help='Output directory (default: logs/<stem>/)')
    args = ap.parse_args()

    if args.latest or not args.logfile:
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        candidates = sorted(
            [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith('.log')],
            key=os.path.getmtime,
        )
        if not candidates:
            sys.exit(f"No .log files found in {log_dir}/")
        logfile = candidates[-1]
        print(f"[INFO] Using latest log: {logfile}")
    else:
        logfile = args.logfile

    if not os.path.exists(logfile):
        sys.exit(f"File not found: {logfile}")

    stem    = os.path.splitext(os.path.basename(logfile))[0]
    out_dir = args.out or os.path.join(os.path.dirname(logfile), stem)

    print(f"[INFO] Parsing {logfile} ...")
    telem, events = parse_log(logfile)

    all_ids = set(telem) | set(events)
    print(f"[INFO] Found {len(all_ids)} drone(s): {', '.join(sorted(all_ids))}. Writing to {out_dir}/\n")
    write_csvs(telem, events, out_dir)
    print("\n[INFO] Done.")


if __name__ == '__main__':
    main()
