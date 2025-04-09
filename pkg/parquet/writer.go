package parquet

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// SplitParquetFile splits a Parquet file into multiple smaller files
func SplitParquetFile(filePath string, numFiles int) error {
	// Open the original file
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer f.Close()

	// Parse the Parquet file
	reader := parquet.NewReader(f)
	defer reader.Close()

	// Get the total number of rows in the file
	totalRows := reader.NumRows()
	if totalRows == 0 {
		return fmt.Errorf("file is empty, no need to split")
	}

	// Calculate the number of rows for each file
	rowsPerFile := int64(math.Ceil(float64(totalRows) / float64(numFiles)))

	// Prepare output file names
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = strings.TrimSuffix(baseName, ext)
	dir := filepath.Dir(filePath)

	// Get the Schema
	schema := reader.Schema()

	// Read all data
	allRows := make([]map[string]interface{}, totalRows)
	rowBuf := make([]parquet.Row, totalRows)
	
	// Reset position to the beginning
	if err := reader.SeekToRow(0); err != nil {
		return fmt.Errorf("failed to seek to the beginning: %v", err)
	}
	
	// Read all rows
	count, err := reader.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read data: %v", err)
	}
	
	// Convert to map format
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		if err := reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
			return fmt.Errorf("failed to convert row data: %v", err)
		}
		allRows[i] = row
	}
	
	// Split data into multiple files
	for i := 0; i < numFiles; i++ {
		startRow := int64(i) * rowsPerFile
		
		// If the starting row exceeds the total number of rows, exit the loop
		if startRow >= int64(count) {
			break
		}
		
		// Calculate the number of rows this slice should contain
		endRow := startRow + rowsPerFile
		if endRow > int64(count) {
			endRow = int64(count)
		}
		
		// Create the output file
		outputPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", baseName, i+1, ext))
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %v", outputPath, err)
		}
		
		// Create the Parquet writer
		writer := parquet.NewWriter(outputFile, schema)
		
		// Write the specified number of rows
		for j := startRow; j < endRow; j++ {
			// Write one row of data
			if err := writer.Write(allRows[j]); err != nil {
				outputFile.Close()
				return fmt.Errorf("failed to write row: %v", err)
			}
		}
		
		// Complete writing and close the file
		if err := writer.Close(); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to close writer: %v", err)
		}
		
		if err := outputFile.Close(); err != nil {
			return fmt.Errorf("failed to close output file: %v", err)
		}
	}

	return nil
} 