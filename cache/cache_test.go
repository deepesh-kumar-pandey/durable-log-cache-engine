package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestCache_SetAndGet(t *testing.T) {
	c := NewCache(100)
	c.Set("k1", []byte("hello"))

	entry, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected cache hit, got miss")
	}
	if string(entry.Value) != "hello" {
		t.Errorf("expected 'hello', got %q", entry.Value)
	}
	if entry.Recovered {
		t.Error("Set() should not mark entry as recovered")
	}
}

func TestCache_SetRecovered(t *testing.T) {
	c := NewCache(100)
	c.SetRecovered("k1", []byte("restored"))

	entry, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected hit after SetRecovered")
	}
	if !entry.Recovered {
		t.Error("SetRecovered() should mark entry as recovered")
	}
	if string(entry.Value) != "restored" {
		t.Errorf("expected 'restored', got %q", entry.Value)
	}
}

func TestCache_Get_Miss(t *testing.T) {
	c := NewCache(100)
	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected miss for unknown key, got hit")
	}
}

func TestCache_LRU_Eviction(t *testing.T) {
	c := NewCache(3)
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))

	// Access b and c so "a" becomes the coldest entry.
	c.Get("b")
	c.Get("c")

	// Adding "d" should evict "a".
	c.Set("d", []byte("4"))

	if _, ok := c.Get("a"); ok {
		t.Error("expected 'a' to be evicted (LRU)")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok := c.Get(k); !ok {
			t.Errorf("expected %q to survive eviction", k)
		}
	}
}

func TestCache_LRU_AccessPromotes(t *testing.T) {
	c := NewCache(3)
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3"))

	// Access "a" to promote it — "b" becomes the new LRU.
	c.Get("a")
	c.Set("d", []byte("4")) // evicts "b"

	if _, ok := c.Get("b"); ok {
		t.Error("expected 'b' to be evicted after 'a' was promoted to MRU")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("expected 'a' to survive — it was just accessed")
	}
}

func TestCache_HitRate(t *testing.T) {
	c := NewCache(100)
	c.Set("x", []byte("v"))

	c.Get("x")       // hit
	c.Get("missing") // miss

	if rate := c.HitRate(); rate != 50.0 {
		t.Errorf("expected 50.0%% hit rate, got %.2f", rate)
	}
}

func TestCache_HitRate_NoLookups(t *testing.T) {
	c := NewCache(100)
	if c.HitRate() != 0 {
		t.Error("expected 0 hit rate with no lookups")
	}
}

func TestCache_Len(t *testing.T) {
	c := NewCache(100)
	if c.Len() != 0 {
		t.Errorf("expected empty cache, got len %d", c.Len())
	}
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	if c.Len() != 2 {
		t.Errorf("expected len 2, got %d", c.Len())
	}
}

func TestCache_Concurrent(t *testing.T) {
	c := NewCache(500)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			c.Set(fmt.Sprintf("key%d", i), fmt.Appendf(nil, "val%d", i))
		}(i)
		go func(i int) {
			defer wg.Done()
			c.Get(fmt.Sprintf("key%d", i))
		}(i)
	}
	wg.Wait()

	if c.Len() > 100 {
		t.Errorf("unexpected cache size %d; expected <= 100", c.Len())
	}
}
