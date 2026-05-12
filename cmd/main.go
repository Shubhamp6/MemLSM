package main

import (
	"fmt"
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

	// test
	e.Put("userid1", []byte("name1"))
	e.Put("userid2", []byte("name2"))

	_, value := e.Get("userid1")
	_, value1 := e.Get("userid2")

	fmt.Printf("Found value: %s\n", value)
	fmt.Printf("Found value: %s\n", value1)

	e.Remove("userid1")
	e.Remove("userid2")

	_, value = e.Get("userid1")
	_, value1 = e.Get("userid2")

	fmt.Printf("Found value: %s\n", value)
	fmt.Printf("Found value: %s\n", value1)
}
