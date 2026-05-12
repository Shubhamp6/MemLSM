package memtable

import (
	"math/rand"
	"mem-lsm/config"
	"sync"
)

type Node struct {
	key       string
	value     []byte
	isDeleted bool
	next      []*Node
}

type SkipList struct {
	head  *Node
	level int
	cfg   *config.Config
	mu    sync.RWMutex
}

func NewSkipList(cfg *config.Config) *SkipList {
	return &SkipList{
		head:  &Node{next: make([]*Node, cfg.SkipListMaxLevel)},
		level: 0,
		cfg:   cfg,
	}
}

func randomLevel(cfg *config.Config) int {
	level := 0
	for rand.Float64() < cfg.SkipListLevelProbability && level < cfg.SkipListMaxLevel {
		level++
	}

	return level
}

func (sl *SkipList) Put(key string, value []byte, isDelete bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*Node, sl.cfg.SkipListMaxLevel)
	cur := sl.head

	for i := sl.level; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}

		update[i] = cur
	}

	cur = cur.next[0]

	if cur != nil && cur.key == key {
		cur.value = value
		cur.isDeleted = isDelete
		return
	}

	randLevel := randomLevel(sl.cfg)

	if randLevel > sl.level {
		for i := sl.level + 1; i <= randLevel; i++ {
			update[i] = sl.head
		}

		sl.level = randLevel
	}

	newNode := &Node{
		key:       key,
		value:     value,
		isDeleted: isDelete,
		next:      make([]*Node, randLevel+1),
	}

	for i := 0; i <= randLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
}

func (sl *SkipList) Get(key string) (bool, bool, string) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	cur := sl.head
	for i := sl.level; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}
	}

	if cur != nil && cur.next[0] != nil && cur.next[0].key == key {
		if cur.next[0].isDeleted {
			return false, true, ""
		} else {
			return true, false, string(cur.next[0].value)
		}
	}

	return false, false, ""
}
