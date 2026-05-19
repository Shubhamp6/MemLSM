package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	helper "mem-lsm/common/helper"
	"mem-lsm/config"
	"mem-lsm/internals/memtable"
	"os"
	"sync"
	"sync/atomic"
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
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Printf("Error opening ss table file: %v", err)
		return nil, err
	}

	return &SSTable{file: f}, err
}

func (sstable *SSTable) Write(flushItems <-chan memtable.Item, ssTableRegistry *SSTableRegistry, fileCount *atomic.Int32) error {
	sstable.mu.Lock()
	defer sstable.mu.Unlock()
	index := []IndexEntry{}
	i := 1
	binaryOffset := 0
	maxKey := ""
	minKey := ""
	currentFileNumber := (*fileCount).Load()
	(*fileCount).Add(1)

	for flushItem := range flushItems {
		curItemSize, err := sstable.writeItems(flushItem.Key, flushItem.Value, flushItem.IsDeleted)

		if err != nil {
			return err
		}

		binaryOffset += curItemSize

		if i%10 == 0 {
			index = append(index, IndexEntry{Key: flushItem.Key, Offset: binaryOffset})
		}

		if i == 1 {
			minKey = flushItem.Key
		}

		maxKey = flushItem.Key
		i++
	}

	fileSize, err := sstable.writeIndex(index)

	if err != nil {
		return err
	}

	ssTableMetadata := SSTableMetadata{
		Action:       uint8(ActionAdd),
		FileNumber:   currentFileNumber,
		MinKey:       minKey,
		MaxKey:       maxKey,
		SizeBytes:    fileSize,
		IsCompacting: false,
	}

	err = ssTableRegistry.AppendFileMetadata(ssTableMetadata)

	return nil

}

func (sstable *SSTable) writeItems(key string, value []byte, isDeleted bool) (int, error) {
	keyBuf := []byte(key)

	binaryOffset := 1 + 4 + len(keyBuf) + 4 + len(value)
	buf := make([]byte, binaryOffset)
	buf[0] = helper.ConvertBoolToByte(isDeleted)

	binary.BigEndian.PutUint32(buf[1:1+4], uint32(len(keyBuf)))
	copy(buf[1+4:1+4+len(keyBuf)], keyBuf)

	valStart := 1 + 4 + len(keyBuf)
	binary.BigEndian.PutUint32(buf[valStart:valStart+4], uint32(len(value)))
	copy(buf[valStart+4:], value)

	_, err := sstable.file.Write(buf)

	if err != nil {
		log.Printf("Error Writing key-value to SS Table: %v", err)
		return 0, err
	}

	return binaryOffset, sstable.file.Sync()
}

func (sstable *SSTable) writeIndex(indexEntries []IndexEntry) (int64, error) {
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
			return 0, err
		}

	}

	footerBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(footerBuf, uint64(indexStartOffset))
	_, err := sstable.file.Write(footerBuf)

	if err != nil {
		log.Printf("Error writing index start offset to ss table file: %v", err)
		return 0, err
	}

	fileSize, err := sstable.file.Seek(0, io.SeekEnd)

	if err != nil {
		log.Printf("Error seeking size of ss table file: %v", err)
		return 0, err
	}

	return fileSize, sstable.file.Sync()
}

func (sstable *SSTable) GetIndexStartOffset() (int64, error) {
	f := sstable.file
	indexStartOffsetBuf := make([]byte, 8)

	currenOffset, err := f.Seek(0, io.SeekCurrent)

	if err != nil {
		return -1, err
	}

	_, err = f.Seek(-8, io.SeekEnd)

	if err != nil {
		return -1, err
	}

	if _, err = io.ReadFull(f, indexStartOffsetBuf); err != nil {
		return -1, err
	}

	indexStartOffset := binary.BigEndian.Uint64(indexStartOffsetBuf)

	_, err = f.Seek(currenOffset, io.SeekStart)

	if err != nil {
		return -1, err
	}

	return int64(indexStartOffset), nil
}

func (sstable *SSTable) GetNextItem(indexStartOffSet int64) (memtable.Item, error) {
	f := sstable.file
	var item memtable.Item
	var err error

	currentOffset, err := f.Seek(0, io.SeekCurrent)

	if err != nil {
		return item, err
	}

	if currentOffset == int64(indexStartOffSet) {
		return item, io.EOF
	}

	tombstoneBuf := make([]byte, 1)
	if _, err = io.ReadFull(f, tombstoneBuf); err != nil {
		return item, err
	}
	item.IsDeleted = helper.ConvertByteToBool(tombstoneBuf[0])

	keyLenBuf := make([]byte, 4)
	if _, err = io.ReadFull(f, keyLenBuf); err != nil {
		return item, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	keyBuf := make([]byte, keyLen)
	if _, err = io.ReadFull(f, keyBuf); err != nil {
		return item, err
	}
	item.Key = string(keyBuf)

	valueLenBuf := make([]byte, 4)
	if _, err = io.ReadFull(f, valueLenBuf); err != nil {
		return item, err
	}
	valueLen := binary.BigEndian.Uint32(valueLenBuf)

	valueBuf := make([]byte, valueLen)
	if _, err = io.ReadFull(f, valueBuf); err != nil {
		return item, err
	}
	item.Value = valueBuf

	return item, nil
}

func (sstable *SSTable) Get(ssTableRegistry *SSTableRegistry, key string, cfg *config.Config) (bool, string) {
	sstable.mu.RLock()
	defer sstable.mu.RUnlock()
	searchSSTables := ssTableRegistry.GetSearchSSTables(key)
	for _, ssTableFileNumber := range searchSSTables {
		f, err := os.Open(cfg.SSTableFilePath + GetSSTableFileNameSuffix(ssTableFileNumber, cfg.SSTableFileSequenceLen))

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

		found, isDeleted, value, err := searchSSTable(f, key, fileSearchStartOffset)

		if err != nil {
			log.Printf("Error finding key in ss table(file number: %d): %v", ssTableFileNumber, err)
			f.Close()
			return false, ""
		}

		if isDeleted {
			f.Close()
			return false, ""
		}

		if found {
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

func searchSSTable(f *os.File, key string, startOffset int) (bool, bool, string, error) {
	_, err := f.Seek(int64(startOffset), io.SeekStart)

	if err != nil {
		return false, false, "", err
	}

	for {
		tombstoneBuf := make([]byte, 1)
		if _, err = io.ReadFull(f, tombstoneBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return false, false, "", nil
			}

			return false, false, "", err
		}
		isDeleted := helper.ConvertByteToBool(tombstoneBuf[0])

		keyLenBuf := make([]byte, 4)
		if _, err = io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return false, false, "", nil
			}

			return false, false, "", err
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)

		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return false, false, "", nil
			}

			return false, false, "", err
		}

		if key == string(keyBuf) && isDeleted {
			return false, true, "", nil
		}

		valueLenBuf := make([]byte, 4)
		if _, err = io.ReadFull(f, valueLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return false, false, "", nil
			}

			return false, false, "", err
		}
		valueLen := binary.BigEndian.Uint32(valueLenBuf)

		if key == string(keyBuf) {
			valueBuf := make([]byte, valueLen)
			if _, err = io.ReadFull(f, valueBuf); err != nil {
				if err == io.ErrUnexpectedEOF {
					return false, false, "", nil
				}

				return false, false, "", err
			}
			return true, false, string(valueBuf), nil
		} else {
			_, err := f.Seek(int64(valueLen), io.SeekCurrent)

			if err != nil {
				return false, false, "", err
			}
		}
	}
}

func (sstable *SSTable) Close() error {
	err := sstable.file.Close()

	if err != nil && !errors.Is(err, os.ErrClosed) && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (sstable *SSTable) Remove() error {
	err := sstable.Close()
	if err != nil {
		log.Printf("Error closing SS table file: %v", err)
		return err
	}
	return os.Remove(sstable.file.Name())
}

func GetSSTableFileNameSuffix(sequenceNumber int32, sequenceLen int) string {
	sequenceNumberString := fmt.Sprintf("%0*d", sequenceLen, sequenceNumber)
	return "-" + sequenceNumberString + ".sst"
}
