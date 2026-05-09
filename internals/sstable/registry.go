package sstable

import (
	"cmp"
	"encoding/binary"
	"io"
	"log"
	"mem-lsm/config"
	"os"
	"slices"
	"sync"
)

const (
	actionBufLen     = 1
	fileNumberBufLen = 4
	keyLenBufLen     = 4
)

type Action uint8

const (
	ActionAdd Action = iota
	ActionDelete
)

type SSTableMetadata struct {
	Action     uint8
	FileNumber int
	MinKey     string
	MaxKey     string
}

type SSTableRegistry struct {
	Manifest *os.File
	Metadata []SSTableMetadata
	mu       sync.RWMutex
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

	minKeyBuf := []byte(metadata.MinKey)
	maxKeyBuf := []byte(metadata.MaxKey)
	buf := make([]byte, actionBufLen+fileNumberBufLen+keyLenBufLen+len(minKeyBuf)+keyLenBufLen+len(maxKeyBuf))

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
	copy(buf[startVal:], maxKeyBuf)

	_, err := sstableregistry.Manifest.Write(buf)

	if err != nil {
		log.Printf("Error writing ss table metadata to manifest file: %v", err)
		return err
	}

	return sstableregistry.Manifest.Sync()
}

func (sstableregistry *SSTableRegistry) RecovertSSTableRegistry() error {
	sstableregistry.mu.RLock()
	defer sstableregistry.mu.RUnlock()

	f, err := os.Open(sstableregistry.Manifest.Name())

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Printf("Error opening ss table Manifest file: %v", err)
		return err
	}

	defer f.Close()

	metadata := []SSTableMetadata{}
	for {
		ssTableMetadata := SSTableMetadata{}

		actionTypeBuf := make([]byte, 1)
		if _, err := io.ReadFull(f, actionTypeBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		ssTableMetadata.Action = uint8(Action(actionTypeBuf[0]))

		fileNumberBuf := make([]byte, fileNumberBufLen)
		if _, err := io.ReadFull(f, fileNumberBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		ssTableMetadata.FileNumber = int(binary.BigEndian.Uint32(fileNumberBuf))

		keyLenBuf := make([]byte, keyLenBufLen)
		if _, err := io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		minKeyLen := binary.BigEndian.Uint32(keyLenBuf)

		minKeyBuf := make([]byte, minKeyLen)

		if _, err := io.ReadFull(f, minKeyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		ssTableMetadata.MinKey = string(minKeyBuf)

		if _, err := io.ReadFull(f, keyLenBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		maxKeyLen := binary.BigEndian.Uint32(keyLenBuf)

		maxKeyBuf := make([]byte, maxKeyLen)

		if _, err := io.ReadFull(f, maxKeyBuf); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		ssTableMetadata.MaxKey = string(maxKeyBuf)

		metadata = append(metadata, ssTableMetadata)
	}

	sstableregistry.Metadata = metadata

	slices.SortFunc(sstableregistry.Metadata, func(a SSTableMetadata, b SSTableMetadata) int {
		return cmp.Compare(a.FileNumber, b.FileNumber)
	})
	return nil
}
