#!/usr/bin/python

import sys

num_array = []

for line in sys.stdin:
	data = line.strip()
	num_array.append(int(data))

num_array = sorted(num_array, reverse=True)[:10]
num_array = sorted(num_array)

for i in range(0, 10):
	print num_array[i]