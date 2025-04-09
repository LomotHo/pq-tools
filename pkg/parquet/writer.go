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
// Note: This function has known issues with certain Parquet files and INT64 encoding
func SplitParquetFile(filePath string, numFiles int) error {
	// Open the source file
	srcFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer srcFile.Close()
	
	// Check if the file has a valid Parquet header
	header := make([]byte, 4)
	_, err = srcFile.Read(header)
	if err != nil {
		return fmt.Errorf("failed to read file header: %v", err)
	}
	
	// Reset file position
	_, err = srcFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to reset file position: %v", err)
	}
	
	// Verify magic header
	if string(header) != "PAR1" {
		return fmt.Errorf("invalid file format: the file is not a valid Parquet file")
	}
	
	// Create Parquet reader
	reader := parquet.NewReader(srcFile)
	if reader == nil {
		return fmt.Errorf("failed to create parquet reader")
	}
	defer reader.Close()
	
	// Get total row count
	totalRows := reader.NumRows()
	if totalRows == 0 {
		return fmt.Errorf("file contains no rows, no need to split")
	}
	
	// Calculate rows per output file
	rowsPerFile := int64(math.Ceil(float64(totalRows) / float64(numFiles)))
	
	// Prepare file name components
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = strings.TrimSuffix(baseName, ext)
	dir := filepath.Dir(filePath)
	
	// Get source file schema
	schema := reader.Schema()
	
	// Split data into multiple files
	for i := 0; i < numFiles; i++ {
		startRow := int64(i) * rowsPerFile
		
		// Exit loop if we've processed all rows
		if startRow >= totalRows {
			break
		}
		
		// Calculate how many rows should be in this slice
		endRow := startRow + rowsPerFile
		if endRow > totalRows {
			endRow = totalRows
		}
		
		// Create output file
		outputPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", baseName, i+1, ext))
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %v", outputPath, err)
		}
		
		// Seek to starting row in source file
		if err := reader.SeekToRow(startRow); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to seek to row %d: %v", startRow, err)
		}
		
		// Create writer with same schema
		writer := parquet.NewWriter(outputFile, schema)
		
		// Process rows in batches
		rowsToWrite := endRow - startRow
		rowCount := int64(0)
		rowBuf := make([]parquet.Row, 100) // Buffer for batch processing
		
		// Read and write rows in batches
		for rowCount < rowsToWrite {
			// Calculate batch size
			batchSize := rowsToWrite - rowCount
			if batchSize > 100 {
				batchSize = 100
			}
			
			// Read a batch of rows
			n, err := reader.ReadRows(rowBuf[:batchSize])
			if err != nil && err != io.EOF {
				outputFile.Close()
				return fmt.Errorf("failed to read rows: %v", err)
			}
			
			if n == 0 {
				break // No more rows to read
			}
			
			// Convert Parquet rows to Go objects before writing to avoid encoding issues
			for i := 0; i < n; i++ {
				// Convert row to map
				row := make(map[string]interface{})
				if err := reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
					outputFile.Close()
					writer.Close()
					return fmt.Errorf("failed to convert row data: %v", err)
				}
				
				// Write converted object
				if err := writer.Write(row); err != nil {
					outputFile.Close()
					writer.Close()
					return fmt.Errorf("failed to write row: %v", err)
				}
			}
			
			rowCount += int64(n)
			
			if err == io.EOF {
				break
			}
		}
		
		// Ensure all data is written and close writer
		if err := writer.Close(); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to close writer: %v", err)
		}
		
		// Close output file
		if err := outputFile.Close(); err != nil {
			return fmt.Errorf("failed to close output file: %v", err)
		}
	}
	
	// Successfully split the file
	fmt.Printf("Successfully split file %s into %d files\n", filePath, numFiles)
	return nil
}