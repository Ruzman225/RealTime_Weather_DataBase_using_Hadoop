#!/usr/bin/env python
"""Reducer: emits city, day count, mean of daily means, lowest daily min, highest daily max."""
import sys

current_city = None
count = 0
sum_mean = 0.0
min_temp = None
max_temp = None

def emit(city, count, total, low, high):
    if city is not None and count:
        print(f"{city}\t{count}\t{total / count:.3f}\t{low:.3f}\t{high:.3f}")

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    city, values = line.split("\t", 1)
    try:
        mean_value, daily_min, daily_max, n = values.split(",")
        mean_value, daily_min, daily_max, n = float(mean_value), float(daily_min), float(daily_max), int(n)
    except ValueError:
        continue
    if current_city is not None and city != current_city:
        emit(current_city, count, sum_mean, min_temp, max_temp)
        count, sum_mean, min_temp, max_temp = 0, 0.0, None, None
    current_city = city
    count += n
    sum_mean += mean_value
    min_temp = daily_min if min_temp is None else min(min_temp, daily_min)
    max_temp = daily_max if max_temp is None else max(max_temp, daily_max)

emit(current_city, count, sum_mean, min_temp, max_temp)
