package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/LomotHo/pq-tools/pkg/parquet"
)

// handleParquetReader creates a Parquet reader and handles potential errors
func handleParquetReader(filePath string) (*parquet.ParquetReader, error) {
	reader, err := parquet.NewParquetReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("Failed to read file: %v", err)
	}
	return reader, nil
}

// handleRowsError processes errors from reader operations and provides user-friendly messages
func handleRowsError(err error) error {
	if err == nil {
		return nil
	}
	
	if err.Error() == "invalid reader: reader is not initialized properly" {
		return fmt.Errorf("Failed to read data: the file appears to be corrupt or invalid")
	}
	return fmt.Errorf("Failed to read data: %v", err)
}

// handleSplitError processes errors from splitting operations
func handleSplitError(err error) error {
	if err == nil {
		return nil
	}
	
	errMsg := err.Error()
	
	// Check for specific error types
	if strings.Contains(errMsg, "failed to create Parquet reader") || 
	   strings.Contains(errMsg, "invalid reader") || 
	   strings.Contains(errMsg, "corrupted") {
		return fmt.Errorf("Failed to split file: the file appears to be corrupt or invalid")
	}
	
	if strings.Contains(errMsg, "file is empty") {
		return fmt.Errorf("Failed to split file: the file is empty")
	}
	
	return fmt.Errorf("Failed to split file: %v", err)
}

// safeClose safely closes a ParquetReader and handles any errors
func safeClose(reader *parquet.ParquetReader) {
	if reader != nil {
		if err := reader.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing file: %v\n", err)
		}
	}
} 