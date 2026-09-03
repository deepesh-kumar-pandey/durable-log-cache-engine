package cache

import (
	"sync"
	"sync/atomic"
)

// lruNode is a doubly-linked list node for the LRU cache.
type lruNode struct {
	key       string
	value     []byte
	recovered bool // true if restored from WAL crash recovery, not a live write
	prev      *lruNode
	next      *lruNode
}

// CacheEntry is returned by Get, carrying the value and provenance metadata.
type CacheEntry struct {
	Value     []byte
	Recovered bool // true when the entry was loaded from a WAL crash recovery replay
}

// Cache is a thread-safe, bounded LRU in-memory index of committed task records.
// When the cache is full, the least-recently-used entry is evicted on Set.
type Cache struct {
	mu      sync.Mutex
	store   map[string]*lruNode
	head    *lruNode // MRU sentinel (dummy)
	tail    *lruNode // LRU sentinel (dummy)
	maxSize int
	size    int

	hits   uint64 // accessed via sync/atomic
	misses uint64 // accessed via sync/atomic
}

// NewCache allocates a bounded LRU cache with the given maximum entry capacity.
func NewCache(maxSize int) *Cache {
	if maxSize <= 0 {
		maxSize = 10_000
	}
	head := &lruNode{}
	tail := &lruNode{}
	head.next = tail
	tail.prev = head
	return &Cache{
		store:   make(map[string]*lruNode, maxSize),
		head:    head,
		tail:    tail,
		maxSize: maxSize,
	}
}

// Set inserts or updates a live (non-recovered) record in the LRU cache.
// If the cache is at capacity the least-recently-used entry is evicted first.
func (c *Cache) Set(key string, value []byte) {
	c.setInternal(key, value, false)
}

// SetRecovered inserts an entry restored during WAL crash recovery.
// These entries are distinguished from live writes via CacheEntry.Recovered.
func (c *Cache) SetRecovered(key string, value []byte) {
	c.setInternal(key, value, true)
}

func (c *Cache) setInternal(key string, value []byte, recovered bool) {
	owned := make([]byte, len(value))
	copy(owned, value)

	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.store[key]; ok {
		// Update in-place and promote to MRU.
		node.value = owned
		node.recovered = recovered
		c.removeNode(node)
		c.insertFront(node)
		return
	}

	// Evict the LRU entry when at capacity.
	if c.size >= c.maxSize {
		lru := c.tail.prev
		c.removeNode(lru)
		delete(c.store, lru.key)
		c.size--
	}

	node := &lruNode{key: key, value: owned, recovered: recovered}
	c.store[key] = node
	c.insertFront(node)
	c.size++
}

// Get performs a thread-safe LRU lookup, promoting the accessed entry to MRU.
// Returns (entry, true) on hit; (zero, false) on miss.
func (c *Cache) Get(key string) (CacheEntry, bool) {
	c.mu.Lock()
	node, ok := c.store[key]
	if ok {
		c.removeNode(node)
		c.insertFront(node)
		entry := CacheEntry{Value: node.value, Recovered: node.recovered}
		c.mu.Unlock()
		atomic.AddUint64(&c.hits, 1)
		return entry, true
	}
	c.mu.Unlock()
	atomic.AddUint64(&c.misses, 1)
	return CacheEntry{}, false
}

// Len returns the current number of entries held in the cache.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// HitRate returns the cache hit percentage (0–100) since process start.
// Returns 0 if no lookups have been issued yet.
func (c *Cache) HitRate() float64 {
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

// insertFront links node immediately after the MRU sentinel head.
func (c *Cache) insertFront(node *lruNode) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

// removeNode unlinks node from its current list position without freeing it.
func (c *Cache) removeNode(node *lruNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}
