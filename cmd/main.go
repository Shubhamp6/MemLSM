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
	e.Put("userid3", []byte("name3"))
	e.Put("userid4", []byte("name4"))
	e.Put("userid5", []byte("name5"))
	e.Put("userid6", []byte("name6"))
	e.Put("userid7", []byte("name7"))
	e.Put("userid8", []byte("name8"))

	_, value1 := e.Get("userid1")
	_, value2 := e.Get("userid2")
	_, value3 := e.Get("userid3")
	_, value4 := e.Get("userid4")
	_, value5 := e.Get("userid5")
	_, value6 := e.Get("userid6")

	fmt.Printf("Found value1: %s\n", value1)
	fmt.Printf("Found value2: %s\n", value2)
	fmt.Printf("Found value3: %s\n", value3)
	fmt.Printf("Found value4: %s\n", value4)
	fmt.Printf("Found value5: %s\n", value5)
	fmt.Printf("Found value6: %s\n", value6)

	e.Remove("userid1")
	e.Remove("userid2")

	_, value3 = e.Get("userid3")
	_, value4 = e.Get("userid4")

	fmt.Printf("Found value3: %s\n", value3)
	fmt.Printf("Found value4: %s\n", value4)
}
