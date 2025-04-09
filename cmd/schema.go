package cmd

import (
	"fmt"
	"path/filepath"
	"strings"
	"github.com/spf13/cobra"
)

// schemaCmd represents the schema command
var schemaCmd = &cobra.Command{
	Use:   "schema [file]",
	Short: "Display schema information of a Parquet file",
	Long:  `Display schema information of a Parquet file, including field names, types, etc.`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		
		// Create Parquet reader with improved error handling
		reader, err := handleParquetReader(filePath)
		if err != nil {
			er(err.Error())
			return
		}
		defer safeClose(reader)

		// Get schema information
		schema, err := reader.GetSchema()
		if err != nil {
			// Provide more specific error messages for common issues
			if err.Error() == "invalid reader: reader is not initialized properly" {
				er("Failed to get schema: the file appears to be corrupt or invalid")
			} else if err.Error() == "failed to get schema: schema is nil" {
				er("Failed to get schema: the file has no schema information")
			} else {
				er(fmt.Sprintf("Failed to get schema: %v", err))
			}
			return
		}

		// Get file size
		fileName := filepath.Base(filePath)
		
		// Print the results with improved header
		fmt.Println(strings.Repeat("=", 50))
		fmt.Printf("  SCHEMA: %s\n", fileName)
		fmt.Println(strings.Repeat("=", 50))
		fmt.Println()
		fmt.Println(schema)
	},
}

func init() {
	rootCmd.AddCommand(schemaCmd)
} 