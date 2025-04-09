package cmd

import (
	"fmt"
	"github.com/LomotHo/pq-tools/pkg/parquet"

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
		
		// Create Parquet reader
		reader, err := parquet.NewParquetReader(filePath)
		if err != nil {
			er(fmt.Sprintf("Failed to read file: %v", err))
			return
		}
		defer reader.Close()

		// Get schema information
		schema, err := reader.GetSchema()
		if err != nil {
			er(fmt.Sprintf("Failed to get schema information: %v", err))
			return
		}

		// Print the results
		fmt.Printf("Schema information for file: %s\n\n%s\n", filePath, schema)
	},
}

func init() {
	rootCmd.AddCommand(schemaCmd)
} 