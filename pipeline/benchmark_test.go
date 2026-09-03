package pipeline

import (
	"testing"
)

func BenchmarkTokenBucket_Allow(b *testing.B) {
	// Practically infinite rate for benchmark to just test Allow() overhead
	tb := NewTokenBucket(1_000_000_000.0, 1_000_000_000.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Allow()
	}
}

func BenchmarkTokenBucket_AllowConcurrent(b *testing.B) {
	tb := NewTokenBucket(1_000_000_000.0, 1_000_000_000.0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}
