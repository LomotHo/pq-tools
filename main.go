package main

import (
	"os"
	"pq-tools/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
} 