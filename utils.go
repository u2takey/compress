package main

import "math"

func SafeAdd(a, b int64) bool {
	return ((a >= 0) && (b <= math.MaxInt64-a)) || ((a < 0) && (b >= math.MinInt64-a))
}

func ZigZagEncode(v int64) uint64 {
	return uint64((v >> 63) ^ (v << 1))
}

func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ -(v & 1))
}
