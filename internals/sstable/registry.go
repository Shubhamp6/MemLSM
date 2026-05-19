package sstable

import (
	"cmp"
	"container/heap"
	"encoding/binary"
	"io"
	"log"
	"math"
	"mem-lsm/common/minheap"
	"mem-lsm/config"
	"mem-lsm/internals/memtable"
	"os"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	actionBufLen     = 1
	fileNumberBufLen = 4
	keyLenBufLen     = 4
	sizeBytesBufLen  = 8
)

type Action uint8

const (
	ActionAdd Action = iota
	ActionDelete
)

type SSTableMetadata struct {
	Action       uint8
	FileNumber   int32
	MinKey       string
	MaxKey       string
	SizeBytes    int64
	IsCompacting bool
}

type SSTableRegistry struct {
	Manifest *os.File
	Metadata []SSTableMetadata
	mu       sync.RWMutex
}

type FileTierMetadata struct {
	NumberOfFiles int
	SizeTier      uint64
	FilesMetadata []SSTableMetadata
}

func NewSSTableRegistry(cfg *config.Config) *SSTableRegistry {
	file, err := os.OpenFile(cfg.SSTableManifestFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening ss table registry manifest file")
		return nil
	}
	return &SSTableRegistry{
		Manifest: file,
		Metadata: make([]SSTableMetadata, 0),
	}
}

func (sstableregistry *SSTableRegistry) AppendFileMetadata(metadata SSTableMetadata) error {
	sstableregistry.mu.Lock()
	defer sstableregistry.mu.Unlock()
	if metadata.Action == uint8(ActionAdd) {
		sstableregistry.Metadata = append(sstableregistry.Metadata, metadata)
	} else {
		for index, fileMetadata := range sstableregistry.Metadata {
			if fileMetadata.FileNumber == metadata.FileNumber {
				sstableregistry.Metadata = append(sstableregistry.Metadata[:index], sstableregistry.Metadata[index+1:]...)
				break
			}
		}
	}

	minKeyBuf := []byte(metadata.MinKey)
	maxKeyBuf := []byte(metadata.MaxKey)
	buf := make([]byte, actionBufLen+fileNumberBufLen+keyLenBufLen+len(minKeyBuf)+keyLenBufLen+len(maxKeyBuf)+sizeBytesBufLen)

	buf[0] = byte(metadata.Action)

	startVal := actionBufLen
	binary.BigEndian.PutUint32(buf[startVal:startVal+fileNumberBufLen], uint32(metadata.FileNumber))

	startVal += fileNumberBufLen
	binary.BigEndian.PutUint32(buf[startVal:startVal+keyLenBufLen], uint32(len(metadata.MinKey)))

	startVal += keyLenBufLen
	copy(buf[startVal:startVal+len(metadata.MinKey)], minKeyBuf)

	startVal += len(minKeyBuf)
	binary.BigEndian.PutUint32(buf[startVal:startVal+keyLenBufLen], uint32(len(metadata.MaxKey)))

	startVal += keyLenBufLen
	copy(buf[startVal:startVal+len(metadata.MaxKey)], maxKeyBuf)

	startVal += len(metadata.MaxKey)
	binary.BigEndian.PutUint64(buf[startVal:startVal+sizeBytesBufLen], uint64(metadata.SizeBytes))

	_, err := sstableregistry.Manifest.Write(buf)

	if err != nil {
		log.Printf("Error writing ss table metadata to manifest file: %v", err)
		return err
	}

	return sstableregistry.Manifest.Sync()
}

func (sstableregistry *SSTableRegistry) RecoverSSTableRegistry() (int32, error) {
	sstableregistry.mu.RLock()
	defer sstableregistry.mu.RUnlock()
	var latestFileNumber int32 = 0

	f, err := os.Open(sstableregistry.Manifest.Name())

	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		log.Printf("Error opening ss table Manifest file: %v", err)
		return 0, err
	}

	defer f.Close()

	metadata := []SSTableMetadata{}
	deletedSSTables := make([]int32, 0)
	for {
		ssTableMetadata := SSTableMetadata{}

		actionTypeBuf := make([]byte, actionBufLen)
		if _, err := io.ReadFull(f, actionTypeBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}
		ssTableMetadata.Action = uint8(Action(actionTypeBuf[0]))

		fileNumberBuf := make([]byte, fileNumberBufLen)
		if _, err := io.ReadFull(f, fileNumberBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return 0, nil
			}
			return 0, err
		}
		ssTableMetadata.FileNumber = int32(binary.BigEndian.Uint32(fileNumberBuf))

		keyLenBuf := make([]byte, keyLenBufLen)
		if _, err := io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}

		minKeyLen := binary.BigEndian.Uint32(keyLenBuf)

		minKeyBuf := make([]byte, minKeyLen)

		if _, err := io.ReadFull(f, minKeyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}
		ssTableMetadata.MinKey = string(minKeyBuf)

		if _, err := io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}

		maxKeyLen := binary.BigEndian.Uint32(keyLenBuf)

		maxKeyBuf := make([]byte, maxKeyLen)

		if _, err := io.ReadFull(f, maxKeyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}
		ssTableMetadata.MaxKey = string(maxKeyBuf)

		sizeBytesBuf := make([]byte, sizeBytesBufLen)

		if _, err := io.ReadFull(f, sizeBytesBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return 0, err
		}
		ssTableMetadata.SizeBytes = int64(binary.BigEndian.Uint64(sizeBytesBuf))
		ssTableMetadata.IsCompacting = false

		latestFileNumber = max(latestFileNumber, ssTableMetadata.FileNumber)
		if ssTableMetadata.Action == uint8(ActionAdd) {
			metadata = append(metadata, ssTableMetadata)
		} else {
			deletedSSTables = append(deletedSSTables, ssTableMetadata.FileNumber)
		}
	}

	for index, ssTableMetadata := range metadata {
		for _, deletedFileNumber := range deletedSSTables {
			if ssTableMetadata.FileNumber == deletedFileNumber {
				metadata = append(metadata[:index], metadata[index+1:]...)
			}
		}
	}

	slices.SortFunc(metadata, func(a SSTableMetadata, b SSTableMetadata) int {
		return cmp.Compare(a.FileNumber, b.FileNumber)
	})

	sstableregistry.Metadata = metadata

	return latestFileNumber, nil
}

func (sstableregistry *SSTableRegistry) GetSearchSSTables(key string) []int32 {
	searchSSTables := []int32{}
	for i := len(sstableregistry.Metadata) - 1; i >= 0; i-- {
		meta := sstableregistry.Metadata[i]
		if meta.MinKey <= key && meta.MaxKey >= key {
			searchSSTables = append(searchSSTables, meta.FileNumber)
		}
	}
	return searchSSTables
}

func (sstableregistry *SSTableRegistry) FindTiersForCompaction(maxFilesPerTier int) []FileTierMetadata {
	fileTiersMetadata := make(map[int]FileTierMetadata)

	for _, metadata := range sstableregistry.Metadata {
		sizeTier := int(math.Pow10(int(math.Log10(float64(metadata.SizeBytes)))))
		if !metadata.IsCompacting {
			if _, ok := fileTiersMetadata[sizeTier]; !ok {
				fileTiersMetadata[sizeTier] = FileTierMetadata{NumberOfFiles: 1, FilesMetadata: []SSTableMetadata{metadata}, SizeTier: uint64(sizeTier)}
			} else {
				currentFileTier := fileTiersMetadata[sizeTier]
				currentFileTier.NumberOfFiles++
				currentFileTier.FilesMetadata = append(currentFileTier.FilesMetadata, metadata)
				fileTiersMetadata[sizeTier] = currentFileTier
			}
		}
	}

	tiersForCompaction := make([]FileTierMetadata, 0)
	for _, fileTierMetadata := range fileTiersMetadata {
		if fileTierMetadata.NumberOfFiles >= maxFilesPerTier {
			tiersForCompaction = append(tiersForCompaction, fileTierMetadata)
		}
	}

	return tiersForCompaction
}

func (sstableregistry *SSTableRegistry) Compact(tierForCompaction FileTierMetadata, currentFileCount *atomic.Int32, cfg *config.Config) error {
	ssTableFiles := make([]struct {
		SSTableMetadata
		*SSTable
		indexStartOffset int64
	}, 0)
	itemIterator := make(chan memtable.Item)
	errCh := make(chan error)

	for _, ssTableMetadata := range tierForCompaction.FilesMetadata {
		sstableregistry.UpdateFileCompactionStatus(ssTableMetadata.FileNumber, true)
		ssTable, err := Open(cfg.SSTableFilePath + GetSSTableFileNameSuffix(ssTableMetadata.FileNumber, cfg.SSTableFileSequenceLen))

		if err != nil {
			log.Printf("Error opening SS table file: %v", err)
			return err
		}

		defer ssTable.Close()

		indexStartOffset, err := ssTable.GetIndexStartOffset()

		if err != nil {
			log.Printf("Error getting SS table index start offset: %v", err)
		}

		ssTableFiles = append(ssTableFiles, struct {
			SSTableMetadata
			*SSTable
			indexStartOffset int64
		}{ssTableMetadata, ssTable, indexStartOffset})
	}

	h := &minheap.MinHeap{}
	heap.Init(h)

	for index, ssTableFile := range ssTableFiles {
		item, err := ssTableFile.GetNextItem(ssTableFile.indexStartOffset)

		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Printf("Error getting next item from ss table file(%d) : %v", ssTableFile.FileNumber, err)
			return err
		}

		heap.Push(h, minheap.HeapItem{Item: item, ItertorIndex: index})
	}

	go func() {
		for {
			if h.Len() == 0 {
				close(itemIterator)
				break
			}

			item := heap.Pop(h).(minheap.HeapItem)

			for {
				if h.Len() == 0 {
					break
				}

				nextItem := heap.Pop(h).(minheap.HeapItem)
				if nextItem.Item.Key != item.Item.Key {
					heap.Push(h, nextItem)
					break
				}

				if ssTableFiles[item.ItertorIndex].FileNumber < ssTableFiles[nextItem.ItertorIndex].FileNumber {
					nextItemFromSSTable, err := ssTableFiles[item.ItertorIndex].GetNextItem(ssTableFiles[item.ItertorIndex].indexStartOffset)

					item = nextItem
					if err != nil {
						if err == io.EOF {
							continue
						}
						log.Printf("Error getting next item from ss table file(%d) : %v", ssTableFiles[item.ItertorIndex].FileNumber, err)
						errCh <- err
						close(itemIterator)
						return
					}
					heap.Push(h, minheap.HeapItem{Item: nextItemFromSSTable, ItertorIndex: item.ItertorIndex})
				}
			}

			if item.Item.IsDeleted == false || tierForCompaction.SizeTier != uint64(cfg.LowestSizeTier) {
				itemIterator <- item.Item
			}

			nextItemFromSSTable, err := ssTableFiles[item.ItertorIndex].GetNextItem(ssTableFiles[item.ItertorIndex].indexStartOffset)

			if err != nil {
				if err == io.EOF {
					continue
				}
				log.Printf("Error getting next item from ss table file(%d) : %v", ssTableFiles[item.ItertorIndex].FileNumber, err)
				errCh <- err
				close(itemIterator)
				return
			}
			heap.Push(h, minheap.HeapItem{Item: nextItemFromSSTable, ItertorIndex: item.ItertorIndex})
		}
		errCh <- nil
	}()

	go func() {
		ssTable, err := Open(cfg.SSTableFilePath + GetSSTableFileNameSuffix((*currentFileCount).Load(), cfg.SSTableFileSequenceLen))

		if err != nil {
			log.Printf("Error opening new SS table file: %v", err)
			errCh <- err
			return
		}

		defer ssTable.Close()

		err = ssTable.Write(itemIterator, sstableregistry, currentFileCount)

		if err != nil {
			log.Printf("Error writing to new SS table file: %v", err)
			errCh <- err
			return
		}

		for index, ssTableFile := range ssTableFiles {

			err = ssTableFile.Remove()

			if err != nil {
				log.Printf("Error getting next item from ss table file(%d) : %v", ssTableFiles[index].FileNumber, err)
			}
			ssTableFile.Action = uint8(ActionDelete)

			sstableregistry.AppendFileMetadata(ssTableFile.SSTableMetadata)
		}
	}()

	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

func (sstableregistry *SSTableRegistry) UpdateFileCompactionStatus(fileNumber int32, compactionStatus bool) {
	metadataLength := len(sstableregistry.Metadata)
	for i := 0; i < metadataLength; i++ {
		if sstableregistry.Metadata[i].FileNumber == fileNumber {
			sstableregistry.Metadata[i].IsCompacting = compactionStatus
			break
		}
	}
}
