package main

import (
	"os"
	"github.com/LomotHo/pq-tools/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
} 