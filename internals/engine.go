package engine

import (
	"log"
	"mem-lsm/config"
	memtable "mem-lsm/internals/memtable"
	sstable "mem-lsm/internals/sstable"
	"mem-lsm/internals/wal"
	"strconv"
	"sync"
)

type Engine struct {
	cfg               *config.Config
	wal               *wal.WAL
	activeMemtable    *memtable.MemTable
	immutableMemtable *memtable.MemTable
	ssTableRegistry   *sstable.SSTableRegistry
	fileCount         int
	mu                sync.Mutex
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	wal, err := wal.Open(cfg.WALFilePath)

	if err != nil {
		return nil, err
	}

	return &Engine{
		cfg:             cfg,
		wal:             wal,
		activeMemtable:  memtable.NewMemTable(cfg),
		ssTableRegistry: sstable.NewSSTableRegistry(cfg),
		fileCount:       0,
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
			e.immutableMemtable = nil
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

	if found == false && e.immutableMemtable != nil {
		found, value = e.immutableMemtable.SkipList.Get(key)
	}

	if found == false {

	}

	return found, value
}

func (e *Engine) Recover() error {
	err := e.wal.RecoverMemoryStore(e.activeMemtable.SkipList)

	if err != nil {
		log.Printf("Error recovering memory store data from WAL file: %v", err)
		return err
	}

	err = e.ssTableRegistry.RecovertSSTableRegistry()

	if err != nil {
		log.Printf("Error recovering ss table registry from manifest file: %v", err)
		return err
	}

	metadataLen := len(e.ssTableRegistry.Metadata)

	if metadataLen > 0 {
		e.fileCount = e.ssTableRegistry.Metadata[metadataLen-1].FileNumber
	}

	return nil
}

func (e *Engine) flushToSSTable() error {
	flushItems := e.immutableMemtable.FlushIterator()

	ssTable, err := sstable.Open(e.cfg.SSTableFilePath + sstable.GetSSTableFileNameSuffix(e.fileCount, e.cfg.SSTableFileSeqeunceLen))

	if err != nil {
		return err
	}

	defer ssTable.Close()

	index := []sstable.IndexEntry{}
	i := 1
	binaryOffset := 0
	maxKey := ""
	minKey := ""

	for flushItem := range flushItems {
		curItemSize, err := ssTable.FlushWriteItems(flushItem.Key, flushItem.Value)

		if err != nil {
			return err
		}

		binaryOffset += curItemSize

		if i%10 == 0 {
			index = append(index, sstable.IndexEntry{Key: flushItem.Key, Offset: binaryOffset})
		}

		if i == 1 {
			minKey = flushItem.Key
		}

		maxKey = flushItem.Key
		i++
	}

	err = ssTable.FlushWriteIndex(index)

	if err != nil {
		return err
	}

	ssTableMetadata := sstable.SSTableMetadata{
		Action:     uint8(sstable.ActionAdd),
		FileNumber: e.fileCount,
		MinKey:     minKey,
		MaxKey:     maxKey,
	}

	print(e.fileCount)

	err = e.ssTableRegistry.AppendFileMetadata(ssTableMetadata)

	e.fileCount++

	return nil
}
