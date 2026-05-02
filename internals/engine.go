package engine

import (
	"mem-lsm/config"
	memtable "mem-lsm/internals/memtable"
	"mem-lsm/internals/wal"
)

type Engine struct {
	wal      *wal.WAL
	memtable *memtable.SkipList
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	wal, err := wal.Open(cfg.WALFilePath)

	if err != nil {
		return nil, err
	}

	return &Engine{
		wal:      wal,
		memtable: memtable.NewSkipList(cfg),
	}, nil
}

func (e *Engine) Put(key string, value []byte) error {
	err := e.wal.Write(key, value)

	if err != nil {
		return err
	}

	e.memtable.Put(key, value)
	return nil
}

func (e *Engine) Get(key string) (bool, []byte) {
	return e.memtable.Get(key)
}

func (e *Engine) Recover() error {
	return e.wal.Recover(e.memtable)
}
