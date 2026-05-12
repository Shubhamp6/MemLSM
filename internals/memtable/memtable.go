package memtable

import "mem-lsm/config"

type MemTable struct {
	SkipList  *SkipList
	sizeBytes int
	cfg       *config.Config
}

type Item struct {
	Key       string
	Value     []byte
	IsDeleted bool
}

func NewMemTable(cfg *config.Config) *MemTable {
	return &MemTable{
		SkipList:  NewSkipList(cfg),
		sizeBytes: 0,
		cfg:       cfg,
	}
}

func (m *MemTable) IsFull(key string, value []byte) bool {
	if m.sizeBytes+len([]byte(key))+len(value) >= m.cfg.MaxMemTableSize {
		return true
	}

	m.sizeBytes += len([]byte(key)) + len(value)
	return false
}

func (m *MemTable) FlushIterator() <-chan Item {
	ch := make(chan Item)

	go func() {
		cur := m.SkipList.head
		for {
			if cur == nil || cur.next[0] == nil {
				break
			}

			ch <- Item{
				Key:       cur.next[0].key,
				Value:     cur.next[0].value,
				IsDeleted: cur.next[0].isDeleted,
			}

			cur = cur.next[0]
		}
		close(ch)
	}()

	return ch
}
