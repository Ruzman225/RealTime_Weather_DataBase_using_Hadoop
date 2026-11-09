#!/usr/bin/env python
"""Reducer: city-month -> days with values, total precipitation mm, average daily mm."""
import sys

current_key = None
count = 0
sum_mm = 0.0

def emit(key, count, total):
    if key is not None and count:
        print(f"{key}\t{count}\t{total:.3f}\t{total / count:.3f}")

for line in sys.stdin:
    line = line.rstrip("\n")
    if "\t" not in line:
        continue
    key, value = line.split("\t", 1)
    try:
        amount = float(value)
    except ValueError:
        continue
    if current_key is not None and key != current_key:
        emit(current_key, count, sum_mm)
        count, sum_mm = 0, 0.0
    current_key = key
    count += 1
    sum_mm += amount

emit(current_key, count, sum_mm)
