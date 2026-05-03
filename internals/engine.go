package engine

import (
	"mem-lsm/config"
	sstable "mem-lsm/internals/SSTable"
	memtable "mem-lsm/internals/memtable"
	"mem-lsm/internals/wal"
	"strconv"
	"sync"
)

type Engine struct {
	cfg               *config.Config
	wal               *wal.WAL
	activeMemtable    *memtable.MemTable
	immutableMemtable *memtable.MemTable
	fileCount         int
	mu                sync.Mutex
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	wal, err := wal.Open(cfg.WALFilePath)

	if err != nil {
		return nil, err
	}

	return &Engine{
		cfg:            cfg,
		wal:            wal,
		activeMemtable: memtable.NewMemTable(cfg),
		fileCount:      0,
	}, nil
}

func (e *Engine) Put(key string, value []byte) error {

	if e.activeMemtable.IsFull(key, value) == true {
		e.mu.Lock()
		e.immutableMemtable = e.activeMemtable
		e.activeMemtable = memtable.NewMemTable(e.cfg)

		archivedWalFilePath := e.cfg.WALRemoveFilePath + "-" + strconv.Itoa(e.fileCount) + ".log"
		newWal, err := e.wal.Rotate(e.cfg.WALFilePath, archivedWalFilePath)

		if err != nil {
			e.mu.Unlock()
			return err
		}

		e.wal = newWal
		e.mu.Unlock()

		// go func() {
		if err := e.flushToSSTable(); err == nil {
			wal.Delete(archivedWalFilePath)
		}
		// }()
	}

	err := e.wal.Write(key, value)

	if err != nil {
		return err
	}

	e.activeMemtable.SkipList.Put(key, value)
	return nil
}

func (e *Engine) Get(key string) (bool, []byte) {
	found, value := e.activeMemtable.SkipList.Get(key)

	if found == false {
		found, value = e.immutableMemtable.SkipList.Get(key)
	}

	return found, value
}

func (e *Engine) Recover() error {
	return e.wal.Recover(e.activeMemtable.SkipList)
}

func (e *Engine) flushToSSTable() error {
	flushItems := e.immutableMemtable.FlushIterator()

	ssTable, err := sstable.Open(e.cfg.SSTableFilePath + "-" + strconv.Itoa(e.fileCount) + ".sst")

	if err != nil {
		return err
	}

	defer ssTable.Close()

	index := []sstable.IndexEntry{}
	i := 1
	binaryOffset := 0

	for flushItem := range flushItems {
		curItemBinaryOffset, err := ssTable.FlushWriteItems(flushItem.Key, flushItem.Value)

		if err != nil {
			return err
		}

		binaryOffset += curItemBinaryOffset

		if i%10 == 0 {
			index = append(index, sstable.IndexEntry{Key: flushItem.Key, Offset: binaryOffset})
		}
		i++
	}

	err = ssTable.FlushWriteIndex(index)

	if err != nil {
		return err
	}

	e.fileCount++

	return nil
}
