package config

import (
	"os"
	"strconv"

	_ "github.com/joho/godotenv/autoload"
)

type Config struct {
	SkipListLevelProbability float64
	SkipListMaxLevel         int
	WALFilePath              string
	WALRemoveFilePath        string
	SSTableFilePath          string
	SSTableManifestFilePath  string
	SSTableFileSequenceLen   int
	MaxMemTableSize          int
	MaxFilesPerTier          int
	LowestSizeTier           int
}

func LoadConfig() Config {
	skipListLevelProbability, _ := strconv.ParseFloat(getEnv("SKIP_LIST_LEVEL_PROBABILITY", "0.5"), 64)
	skipListMaxLevel, _ := strconv.ParseInt(getEnv("SKIP_LIST_MAX_LEVEL", "16"), 10, 64)
	maxMemTableSize, _ := strconv.ParseInt(getEnv("MAX_MEM_TABLE_SIZE", "11"), 10, 64)
	SSTableFileSequenceLen, _ := strconv.ParseInt(getEnv("SS_TABLE_FILE_SEQUENCE_LENGTH", "8"), 10, 64)
	maxFilesPerTier, _ := strconv.ParseInt(getEnv("MAX_FILES_PER_TIER", "4"), 10, 64)
	lowestSizeTier, _ := strconv.ParseInt(getEnv("LOWEST_SIZE_TIER", "1"), 10, 64)
	return Config{
		SkipListLevelProbability: skipListLevelProbability,
		SkipListMaxLevel:         int(skipListMaxLevel),
		WALFilePath:              getEnv("WAL_FILE_PATH", "./wal.log"),
		WALRemoveFilePath:        getEnv("WAL_REMOVE_FILE_PATH", "./wal-remove"),
		SSTableFilePath:          getEnv("SS_TABLE_FILE_PATH", "./sstable"),
		SSTableManifestFilePath:  getEnv("SS_TABLE_MANIFEST_FILE_PATH", "./MANIFEST"),
		SSTableFileSequenceLen:   int(SSTableFileSequenceLen),
		MaxMemTableSize:          int(maxMemTableSize),
		MaxFilesPerTier:          int(maxFilesPerTier),
		LowestSizeTier:           int(lowestSizeTier),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return fallback
}
