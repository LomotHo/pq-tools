package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Optional bool   `json:"optional,omitempty"`
}

// Define fixed data structure for generating test data
type ParquetData struct {
	ID     int64   `parquet:"name=id"`
	Name   string  `parquet:"name=name"`
	Age    int32   `parquet:"name=age"`
	Active bool    `parquet:"name=active"`
	Weight float32 `parquet:"name=weight"`
}

var generateCmd = &cobra.Command{
	Use:   "generate [output]",
	Short: "Generate test Parquet files",
	Long:  `Generate test Parquet files with specified number of rows and custom schema`,
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize random number generator
		rand.Seed(time.Now().UnixNano())

		// Get output path
		outputPath := "./.tmp/test.parquet"
		if len(args) > 0 {
			outputPath = args[0]
		}

		// Ensure output directory exists
		dir := filepath.Dir(outputPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("Failed to create directory: %v\n", err)
			return
		}

		// Get flag values
		rowCount, _ := cmd.Flags().GetInt("rows")
		
		// Create output file
		file, err := os.Create(outputPath)
		if err != nil {
			fmt.Printf("Failed to create file: %v\n", err)
			return
		}
		defer file.Close()
		
		// Create writer
		writer := parquet.NewWriter(file)
		
		// Generate and write data
		for i := 0; i < rowCount; i++ {
			data := ParquetData{
				ID:     int64(i),
				Name:   fmt.Sprintf("User%d", i),
				Age:    int32(20 + rand.Intn(50)),
				Active: rand.Intn(2) == 1,
				Weight: float32(50 + rand.Intn(50)),
			}
			
			if err := writer.Write(&data); err != nil {
				fmt.Printf("Failed to write data: %v\n", err)
				return
			}
		}
		
		// Close writer
		if err := writer.Close(); err != nil {
			fmt.Printf("Failed to close writer: %v\n", err)
			return
		}
		
		fmt.Printf("Successfully generated %d rows of data to %s\n", rowCount, outputPath)
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)
	generateCmd.Flags().IntP("rows", "r", 100, "Number of rows to generate")
	generateCmd.Flags().StringP("schema", "s", "", "Custom schema in JSON format (not currently supported)")
} 