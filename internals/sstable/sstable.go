package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"mem-lsm/config"
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

func (sstable *SSTable) Get(ssTableRegistry *SSTableRegistry, key string, cfg *config.Config) (bool, string) {
	sstable.mu.RLock()
	defer sstable.mu.RUnlock()
	searchSSTables := ssTableRegistry.GetSearchSSTables(key)

	for _, ssTableFileNumber := range searchSSTables {
		f, err := os.Open(cfg.SSTableFilePath + GetSSTableFileNameSuffix(ssTableFileNumber, cfg.SSTableFileSeqeunceLen))

		if err != nil {
			log.Printf("Error opening ss table file(file number: %d) with error: %v", ssTableFileNumber, err)
			f.Close()
			return false, ""
		}

		fileSearchStartOffset, err := searchIndex(f, key)

		if err != nil {
			log.Printf("Error finding search offset in ss table(file number: %d) index: %v", ssTableFileNumber, err)
			f.Close()
			return false, ""
		}

		value, err := searchSSTable(f, key, fileSearchStartOffset)

		if err != nil {
			log.Printf("Error finding key in ss table(file number: %d): %v", ssTableFileNumber, err)
			f.Close()
			return false, ""
		}

		if value != "" {
			f.Close()
			return true, value
		}

		f.Close()
	}

	return false, ""
}

func searchIndex(f *os.File, key string) (int, error) {
	indexStartOffset, err := f.Seek(-8, io.SeekEnd)

	if err != nil {
		log.Printf("Error getting index start offset")
		return -1, err
	}

	indexStartOffsetBuf := make([]byte, 8)

	if _, err := io.ReadFull(f, indexStartOffsetBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1, nil
		}

		return -1, err
	}

	_, err = f.Seek(int64(binary.BigEndian.Uint64(indexStartOffsetBuf)), io.SeekStart)
	searchFileOffset := 0
	for {
		currentOffset, err := f.Seek(0, io.SeekCurrent)

		if err != nil {
			return -1, err
		}

		if currentOffset == indexStartOffset {
			return searchFileOffset, err
		}

		keyLenBuf := make([]byte, 4)
		if _, err = io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return searchFileOffset, nil
			}

			return -1, err
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)

		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return searchFileOffset, nil
			}

			return -1, nil
		}

		if string(keyBuf) > key {
			return searchFileOffset, nil
		}

		searchOffsetBuf := make([]byte, 8)

		if _, err = io.ReadFull(f, searchOffsetBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return searchFileOffset, nil
			}

			return -1, err
		}

		searchFileOffset = int(binary.BigEndian.Uint64(searchOffsetBuf))
	}
}

func searchSSTable(f *os.File, key string, startOffset int) (string, error) {
	_, err := f.Seek(int64(startOffset), io.SeekStart)

	if err != nil {
		return "", err
	}

	for {
		keyLenBuf := make([]byte, 4)
		if _, err = io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return "", nil
			}

			return "", err
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)

		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return "", nil
			}

			return "", err
		}

		valueLenBuf := make([]byte, 4)
		if _, err = io.ReadFull(f, valueLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return "", nil
			}

			return "", err
		}
		valueLen := binary.BigEndian.Uint32(valueLenBuf)

		if key == string(keyBuf) {
			valueBuf := make([]byte, valueLen)
			if _, err = io.ReadFull(f, valueBuf); err != nil {
				if err == io.ErrUnexpectedEOF {
					return "", nil
				}

				return "", err
			}
			return string(valueBuf), nil
		} else {
			_, err := f.Seek(int64(valueLen), io.SeekCurrent)

			if err != nil {
				return "", err
			}
		}
	}
}

func (sstable *SSTable) Close() error {
	return sstable.file.Close()
}

func GetSSTableFileNameSuffix(sequenceNumber int, sequenceLen int) string {
	sequenceNumberString := fmt.Sprintf("%0*d", sequenceLen, sequenceNumber)

	return "-" + sequenceNumberString + ".sst"
}
