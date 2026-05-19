package engine

import (
	"log"
	"mem-lsm/config"
	memtable "mem-lsm/internals/memtable"
	sstable "mem-lsm/internals/sstable"
	"mem-lsm/internals/wal"
	"strconv"
	"sync"
	"sync/atomic"
)

type Engine struct {
	cfg               *config.Config
	wal               *wal.WAL
	activeMemtable    *memtable.MemTable
	immutableMemtable *memtable.MemTable
	ssTableRegistry   *sstable.SSTableRegistry
	fileCount         atomic.Int32
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
		fileCount:       atomic.Int32{},
	}, nil
}

func (e *Engine) addEntry(key string, value []byte, isDelete bool) error {
	if e.activeMemtable.IsFull(key, value) == true {
		e.mu.Lock()
		e.immutableMemtable = e.activeMemtable
		e.activeMemtable = memtable.NewMemTable(e.cfg)

		archivedWalFilePath := e.cfg.WALRemoveFilePath + "-" + strconv.Itoa(int(e.fileCount.Load())) + ".log"
		newWal, err := e.wal.Rotate(e.cfg.WALFilePath, archivedWalFilePath)

		if err != nil {
			e.mu.Unlock()
			return err
		}

		e.wal = newWal
		e.mu.Unlock()

		if err := e.flushToSSTable(); err == nil {
			wal.Delete(archivedWalFilePath)
			e.immutableMemtable = nil
		}
	}

	err := e.wal.Write(key, value, isDelete)

	if err != nil {
		return err
	}

	e.activeMemtable.SizeBytes += 1 + len([]byte(key)) + len(value)
	e.activeMemtable.SkipList.Put(key, value, isDelete)
	return nil
}

func (e *Engine) Put(key string, value []byte) error {
	return e.addEntry(key, value, false)
}

func (e *Engine) Get(key string) (bool, string) {
	found, isDeleted, value := e.activeMemtable.SkipList.Get(key)

	if isDeleted {
		return false, ""
	}

	if found == false && e.immutableMemtable != nil {
		found, isDeleted, value = e.immutableMemtable.SkipList.Get(key)
	}

	if isDeleted {
		return false, ""
	}

	if found == false {
		ssTable := &sstable.SSTable{}
		found, value = ssTable.Get(e.ssTableRegistry, key, e.cfg)
	}

	return found, value
}

func (e *Engine) Remove(key string) error {
	return e.addEntry(key, nil, true)
}

func (e *Engine) Recover() error {
	err := e.wal.RecoverMemoryStore(e.activeMemtable.SkipList)

	if err != nil {
		log.Printf("Error recovering memory store data from WAL file: %v", err)
		return err
	}

	latestFileNumber, err := e.ssTableRegistry.RecoverSSTableRegistry()

	if err != nil {
		log.Printf("Error recovering ss table registry from manifest file: %v", err)
		return err
	}

	e.fileCount.Store(latestFileNumber + 1)

	return nil
}

func (e *Engine) flushToSSTable() error {
	flushItems := e.immutableMemtable.FlushIterator()

	ssTable, err := sstable.Open(e.cfg.SSTableFilePath + sstable.GetSSTableFileNameSuffix(e.fileCount.Load(), e.cfg.SSTableFileSequenceLen))

	if err != nil {
		return err
	}

	defer ssTable.Close()
	err = ssTable.Write(flushItems, e.ssTableRegistry, &e.fileCount)

	if err != nil {
		return err
	}

	go func() {
		e.compact()
	}()

	return nil
}

func (e *Engine) compact() {
	tiersForCompaction := e.ssTableRegistry.FindTiersForCompaction(e.cfg.MaxFilesPerTier)

	for _, tierForCompaction := range tiersForCompaction {
		err := e.ssTableRegistry.Compact(tierForCompaction, &e.fileCount, e.cfg)

		if err != nil {
			log.Printf("Error compacting level(%d): %v", tierForCompaction.SizeTier, err)
		}
	}
}
