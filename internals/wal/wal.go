package wal

import (
	"encoding/binary"
	"io"
	"log"
	helper "mem-lsm/common"
	"mem-lsm/internals/memtable"
	"os"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Printf("Error Opening the WAL file: %v", err)
		return nil, err
	}

	return &WAL{file: f}, nil
}

func Delete(path string) error {
	return os.Remove(path)
}

func (w *WAL) Write(key string, value []byte, isDelete bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keyBuf := []byte(key)
	buf := make([]byte, 1+4+len(keyBuf)+4+len(value))

	tombstoneBuf := helper.ConvertBoolToByte(isDelete)

	buf[0] = tombstoneBuf

	binary.BigEndian.PutUint32(buf[1:1+4], uint32(len(keyBuf)))
	copy(buf[1+4:1+4+len(keyBuf)], keyBuf)

	valStart := 1 + 4 + len(keyBuf)
	binary.BigEndian.PutUint32(buf[valStart:4+valStart], uint32(len(value)))
	copy(buf[4+valStart:], value)

	_, err := w.file.Write(buf)

	if err != nil {
		log.Printf("Error Writing key-value to WAL: %v", err)
		return err
	}

	return w.file.Sync()
}

func (w *WAL) RecoverMemoryStore(sl *memtable.SkipList) error {
	f, err := os.Open(w.file.Name())

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Printf("Error opening WAL file: %v", err)
		return err
	}

	defer f.Close()

	for {
		tombstoneBuf := make([]byte, 1)

		if _, err := io.ReadFull(f, tombstoneBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}

			return err
		}
		tombstone := helper.ConvertByteToBool(tombstoneBuf[0])
		lenBuf := make([]byte, 4)

		if _, err := io.ReadFull(f, lenBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		keyLen := binary.BigEndian.Uint32(lenBuf)

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		if _, err := io.ReadFull(f, lenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		valueLen := binary.BigEndian.Uint32(lenBuf)

		valueBuf := make([]byte, valueLen)

		if _, err := io.ReadFull(f, valueBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		sl.Put(string(keyBuf), valueBuf, tombstone)
	}
}

func (w *WAL) Rotate(path string, deleteWALFilePath string) (*WAL, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.Close()

	if err != nil {
		log.Printf("Error closing WAL file: %v", err)
		return nil, err
	}

	err = os.Rename(path, deleteWALFilePath)

	if err != nil {
		log.Printf("Error renaming WAL file: %v", err)
		return nil, err
	}

	wal, err := Open(path)

	if err != nil {
		return nil, err
	}

	return wal, err
}

func (w *WAL) Close() error {
	return w.file.Close()
}
