package main

import (
	"log"
	"mem-lsm/config"
	engine "mem-lsm/internals"
)

func main() {
	cfg := config.LoadConfig()
	e, err := engine.NewEngine(&cfg)

	err = e.Recover()

	if err != nil {
		log.Printf("Error recovering old on memory data: %v", err)
	}

	if err != nil {
		log.Printf("Error initializing Engine: %v", err)
	}
	e.Put("key1", []byte("value1"))
}
