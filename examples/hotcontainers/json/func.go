package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func main() {
	for {
		var j struct {
			Payload string
		}

		if err := json.NewDecoder(os.Stdin).Decode(&j); err != nil {
			json.NewEncoder(os.Stdout).Encode(err)
			continue
		}

		if err := json.NewEncoder(os.Stdout).Encode(fmt.Sprintln("Hello", j.Payload)); err != nil {
			log.Println(err)
		}
	}
}
