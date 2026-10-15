#!/usr/bin/env python3
"""Hadoop Streaming reducer: sum counters for each sorted composite key."""

import sys


current_key = None
current_total = 0

for line in sys.stdin:
    key, value = line.rstrip("\n").rsplit("\t", 1)
    count = int(value)
    if key == current_key:
        current_total += count
        continue
    if current_key is not None:
        print(f"{current_key}\t{current_total}")
    current_key = key
    current_total = count

if current_key is not None:
    print(f"{current_key}\t{current_total}")
