package sstable

import (
	"encoding/binary"
	"log"
	"os"
	"strconv"
	"sync"
)

type SSTable struct {
	file *os.File
	mu   sync.RWMutex
}

func Open(path string) (*SSTable, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Printf("Error opening ss table file: %v", err)
		return nil, err
	}

	return &SSTable{file: f}, err
}

func (sstable *SSTable) FlushWriteItems(key string, value []byte) (int, error) {
	sstable.mu.Lock()
	defer sstable.mu.Unlock()

	keyBuf := []byte(key)

	binaryOffset := 4 + len(keyBuf) + 4 + len(value)
	buf := make([]byte, binaryOffset)

	binary.BigEndian.PutUint32(buf[0:4], uint32(len(keyBuf)))
	copy(buf[4:4+len(keyBuf)], keyBuf)

	valStart := 4 + len(keyBuf)
	binary.BigEndian.PutUint32(buf[valStart:valStart+4], uint32(len(value)))
	copy(buf[valStart+4:], value)

	_, err := sstable.file.Write(buf)

	if err != nil {
		log.Printf("Error Writing key-value to SS Table: %v", err)
		return 0, err
	}

	return binaryOffset, sstable.file.Sync()
}

func (sstable *SSTable) FlushWriteIndex(index []int) error {
	sstable.mu.Lock()
	defer sstable.mu.Unlock()

	for _, offset := range index {
		keyBuf := []byte(strconv.Itoa(offset))

		buf := make([]byte, 4+len(keyBuf))

		binary.BigEndian.PutUint32(buf, uint32(len(keyBuf)))
		copy(buf[4:], keyBuf)

		_, err := sstable.file.Write(buf)

		if err != nil {
			log.Printf("Error Writing index key to SS Table: %v", err)
			return err
		}

	}
	return sstable.file.Sync()
}

func (sstable *SSTable) Close() error {
	return sstable.file.Close()
}
