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

	// Check if the file is in Parquet format
	// Parquet files have "PAR1" magic header
	header := make([]byte, 4)
	_, err = f.Read(header)
	if err != nil {
		return fmt.Errorf("failed to read file header: %v", err)
	}

	// Reset file pointer to beginning
	_, err = f.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to reset file position: %v", err)
	}

	// Check magic header, provide friendly error for non-Parquet files
	if string(header) != "PAR1" {
		return fmt.Errorf("invalid file format: the file is not a valid Parquet file")
	}

	// Parse the Parquet file with proper error handling
	var reader *parquet.Reader
	
	// Use defer/recover to catch all possible panics
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to process Parquet file: the file might be corrupted or not a valid Parquet file")
		}
	}()
	
	reader = parquet.NewReader(f)
	defer reader.Close()
	
	// Check if reader is nil
	if reader == nil {
		return fmt.Errorf("failed to create Parquet reader: the file might be corrupted")
	}

	// Get the total number of rows in the file
	var totalRows int64
	func() {
		defer func() {
			if r := recover(); r != nil {
				totalRows = 0
			}
		}()
		totalRows = reader.NumRows()
	}()
	
	if totalRows == 0 {
		return fmt.Errorf("file is empty or corrupted, no need to split")
	}

	// Calculate the number of rows for each file
	rowsPerFile := int64(math.Ceil(float64(totalRows) / float64(numFiles)))

	// Prepare output file names
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = strings.TrimSuffix(baseName, ext)
	dir := filepath.Dir(filePath)

	// Get the Schema safely
	var schema parquet.Schema
	var schemaErr error
	
	func() {
		defer func() {
			if r := recover(); r != nil {
				schemaErr = fmt.Errorf("failed to get schema: the file might be corrupted")
			}
		}()
		schema = reader.Schema()
	}()
	
	if schemaErr != nil {
		return schemaErr
	}
	
	if schema == nil {
		return fmt.Errorf("invalid schema: the file might be corrupted")
	}

	// Read all data safely
	allRows := make([]map[string]interface{}, totalRows)
	rowBuf := make([]parquet.Row, totalRows)
	
	// Reset position to the beginning
	seekErr := func() error {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("failed to seek: the file might be corrupted")
			}
		}()
		return reader.SeekToRow(0)
	}()
	
	if seekErr != nil {
		return fmt.Errorf("failed to seek to the beginning: %v", seekErr)
	}
	
	// Read all rows with error handling
	var count int
	var readErr error
	
	func() {
		defer func() {
			if r := recover(); r != nil {
				readErr = fmt.Errorf("failed to read data: the file might be corrupted")
			}
		}()
		count, readErr = reader.ReadRows(rowBuf)
	}()
	
	if readErr != nil && readErr != io.EOF {
		return fmt.Errorf("failed to read data: %v", readErr)
	}
	
	// Convert to map format safely
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		reconErr := func() error {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("failed to convert row: the file might be corrupted")
				}
			}()
			return reader.Schema().Reconstruct(&row, rowBuf[i])
		}()
		
		if reconErr != nil {
			return fmt.Errorf("failed to convert row data: %v", reconErr)
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