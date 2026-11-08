package main

import (
	"log"
	"os"

	"github.com/MeguruMacabre/fetchqueue/internal/cli"
)

func main() {
	err := cli.Run(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
}
