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
	WALFileName              string
}

func LoadConfig() Config {
	skipListLevelProbability, _ := strconv.ParseFloat(getEnv("SKIP_LIST_LEVEL_PROBABILITY", "0.5"), 64)
	skipListMaxLevel, _ := strconv.ParseInt(getEnv("SKIP_LIST_MAX_LEVEL", "16"), 10, 64)
	return Config{
		SkipListLevelProbability: skipListLevelProbability,
		SkipListMaxLevel:         int(skipListMaxLevel),
		WALFilePath:              getEnv("WAL_FILE_PATH", "./wal.log"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return fallback
}
