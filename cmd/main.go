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

	//test
	// e.Put("userid1", []byte("name1"))
	// e.Put("userid2", []byte("name2"))

	// _, value := e.Get("userid1")

	// fmt.Printf("Found value: %s", value)
}
