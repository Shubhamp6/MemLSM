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

	//test flushing
	e.Put("userid", []byte("name"))
	e.Put("userid", []byte("name"))
}
