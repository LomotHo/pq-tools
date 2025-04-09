package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pq",
	Short: "pq is a tool for working with Parquet files",
	Long: `A simple and easy-to-use Parquet file processing toolkit,
allowing you to work with Parquet files just like JSONL files.
Supports viewing header data, tail data, counting rows, and splitting files.`,
}

// Execute executes the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// You can add global flags here
}

func er(msg interface{}) {
	fmt.Println("Error:", msg)
	os.Exit(1)
} 