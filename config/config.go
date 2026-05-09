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
	SSTableFileSeqeunceLen   int
	MaxMemTableSize          int
}

func LoadConfig() Config {
	skipListLevelProbability, _ := strconv.ParseFloat(getEnv("SKIP_LIST_LEVEL_PROBABILITY", "0.5"), 64)
	skipListMaxLevel, _ := strconv.ParseInt(getEnv("SKIP_LIST_MAX_LEVEL", "16"), 10, 64)
	maxMemTableSize, _ := strconv.ParseInt(getEnv("MAX_MEM_TABLE_SIZE", "11"), 10, 64)
	ssTableFileSeqeunceLen, _ := strconv.ParseInt(getEnv("SS_TABLE_FILE_SEQUENCE_LENGTH", "8"), 10, 64)
	return Config{
		SkipListLevelProbability: skipListLevelProbability,
		SkipListMaxLevel:         int(skipListMaxLevel),
		WALFilePath:              getEnv("WAL_FILE_PATH", "./wal.log"),
		WALRemoveFilePath:        getEnv("WAL_REMOVE_FILE_PATH", "./wal-remove"),
		SSTableFilePath:          getEnv("SS_TABLE_FILE_PATH", "./sstable"),
		SSTableManifestFilePath:  getEnv("SS_TABLE_MANIFEST_FILE_PATH", "./MANIFEST"),
		SSTableFileSeqeunceLen:   int(ssTableFileSeqeunceLen),
		MaxMemTableSize:          int(maxMemTableSize),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return fallback
}
