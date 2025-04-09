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
	// Use our ParquetReader that already has error handling
	reader, err := NewParquetReader(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer reader.Close()

	// Get the total number of rows in the file
	totalRows, err := reader.Count()
	if err != nil {
		return fmt.Errorf("failed to count rows: %v", err)
	}
	
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

	// Open the original file directly to work with the underlying Parquet format
	originalFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open original file: %v", err)
	}
	defer originalFile.Close()

	// Create a parquet reader directly
	pReader := parquet.NewReader(originalFile)
	defer pReader.Close()
	
	// Get the schema from the original file
	schema := pReader.Schema()
	
	// Split data into multiple files
	for i := 0; i < numFiles; i++ {
		startRow := int64(i) * rowsPerFile
		
		// If the starting row exceeds the total number of rows, exit the loop
		if startRow >= totalRows {
			break
		}
		
		// Calculate the number of rows this slice should contain
		endRow := startRow + rowsPerFile
		if endRow > totalRows {
			endRow = totalRows
		}
		
		// Create the output file
		outputPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", baseName, i+1, ext))
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %v", outputPath, err)
		}
		
		// Create writer with the same schema as the original file
		writer := parquet.NewWriter(outputFile, schema)
		
		// Position the reader at the start row
		if err := pReader.SeekToRow(startRow); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to seek to row %d: %v", startRow, err)
		}
		
		// Calculate number of rows to read
		rowCount := endRow - startRow
		rows := make([]parquet.Row, rowCount)
		
		// Read the rows
		count, err := pReader.ReadRows(rows)
		if err != nil && err != io.EOF {
			outputFile.Close()
			writer.Close()
			return fmt.Errorf("failed to read rows: %v", err)
		}
		
		// Write the rows to the output file
		for j := 0; j < count; j++ {
			if err := writer.WriteRow(rows[j]); err != nil {
				outputFile.Close()
				writer.Close()
				return fmt.Errorf("failed to write row: %v", err)
			}
		}
		
		// Close the writer and file
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