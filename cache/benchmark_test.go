package cache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkCache_Set(b *testing.B) {
	c := NewCache(10000)
	payload := []byte("benchmark_payload_data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("key-%d", i), payload)
	}
}

func BenchmarkCache_Get(b *testing.B) {
	c := NewCache(10000)
	payload := []byte("benchmark_payload_data")
	for i := 0; i < 1000; i++ {
		c.Set(fmt.Sprintf("key-%d", i), payload)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkCache_SetGetConcurrent(b *testing.B) {
	c := NewCache(10000)
	payload := []byte("benchmark_payload_data")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			c.Set(key, payload)
			c.Get(key)
			i++
		}
	})
}

func BenchmarkWAL_Write(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench_write.wal")
	wal, err := NewWAL(walPath)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	payload := []byte("benchmark_payload_1234567890")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wal.Write(payload); err != nil {
			b.Fatalf("write failed: %v", err)
		}
	}
}

func BenchmarkWAL_WriteAndSync(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench_sync.wal")
	wal, err := NewWAL(walPath)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	payload := []byte("benchmark_payload_1234567890")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wal.Write(payload); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		if err := wal.Sync(); err != nil {
			b.Fatalf("sync failed: %v", err)
		}
	}
}
