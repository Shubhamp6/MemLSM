package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type SSTable struct {
	file *os.File
	mu   sync.RWMutex
}

type IndexEntry struct {
	Key    string
	Offset int
}

type SSTableRegistry []string

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

func (sstable *SSTable) FlushWriteIndex(indexEntries []IndexEntry) error {
	sstable.mu.Lock()
	defer sstable.mu.Unlock()

	indexStartOffset, _ := sstable.file.Seek(0, io.SeekCurrent)
	for _, indexEntry := range indexEntries {
		keyBuf := []byte(indexEntry.Key)
		buf := make([]byte, 4+len(keyBuf)+8)

		binary.BigEndian.PutUint32(buf[0:4], uint32(len(keyBuf)))
		copy(buf[4:4+len(keyBuf)], keyBuf)

		binary.BigEndian.PutUint64(buf[4+len(keyBuf):], uint64(indexEntry.Offset))

		_, err := sstable.file.Write(buf)

		if err != nil {
			log.Printf("Error Writing index key to SS Table: %v", err)
			return err
		}

	}

	footerBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(footerBuf, uint64(indexStartOffset))
	_, err := sstable.file.Write(footerBuf)

	if err != nil {
		log.Printf("Error writing index start offset to ss table file: %v", err)
		return err
	}

	return sstable.file.Sync()
}

func (sstable *SSTable) Close() error {
	return sstable.file.Close()
}

func GetSSTableFileNameSuffix(sequenceNumber int, sequenceLen int) string {
	sequenceNumberString := fmt.Sprintf("%0*d", sequenceLen, sequenceNumber)

	return "-" + sequenceNumberString + ".sst"
}
