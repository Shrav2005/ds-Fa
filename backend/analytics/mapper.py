#!/usr/bin/env python3
"""Hadoop Streaming mapper: JSON analytics record to dimension counters."""

import json
import sys

try:
    from .streaming_common import map_record
except ImportError:
    from streaming_common import map_record


for line in sys.stdin:
    record = json.loads(line)
    for key, value in map_record(record):
        print(f"{key}\t{value}")
