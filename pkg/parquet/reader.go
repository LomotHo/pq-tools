package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

// ParquetReader is a reader for Parquet files
type ParquetReader struct {
	reader *parquet.Reader
	file   *os.File  // Used to close the underlying file
	rowNum int64
}

// NewParquetReader creates a new Parquet file reader
func NewParquetReader(filepath string) (*ParquetReader, error) {
	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	// Create Parquet reader
	reader := parquet.NewReader(file)
	rowNum := reader.NumRows()

	return &ParquetReader{
		reader: reader,
		file:   file,
		rowNum: rowNum,
	}, nil
}

// Head returns the first n rows of the file
func (r *ParquetReader) Head(n int) ([]map[string]interface{}, error) {
	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Limit the number of rows to not exceed the total rows in the file
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// Reset to the beginning of the file
	if err := r.reader.SeekToRow(0); err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning of the file: %v", err)
	}

	// Read the first n rows
	result := make([]map[string]interface{}, 0, n)
	rowBuf := make([]parquet.Row, n)
	
	count, err := r.reader.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read row data: %v", err)
	}
	
	// Convert to map format
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		if err := r.reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
			return nil, fmt.Errorf("failed to convert row data: %v", err)
		}
		result = append(result, row)
	}

	return result, nil
}

// Tail returns the last n rows of the file
func (r *ParquetReader) Tail(n int) ([]map[string]interface{}, error) {
	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Limit the number of rows to not exceed the total rows in the file
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// Determine which row to start reading from
	startRow := r.rowNum - int64(n)
	if startRow < 0 {
		startRow = 0
	}
	
	// Reset position to the specified row
	if err := r.reader.SeekToRow(startRow); err != nil {
		return nil, fmt.Errorf("failed to seek to specified row: %v", err)
	}

	// Read the last n rows
	result := make([]map[string]interface{}, 0, n)
	rowBuf := make([]parquet.Row, n)
	
	count, err := r.reader.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read row data: %v", err)
	}
	
	// Convert to map format
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		if err := r.reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
			return nil, fmt.Errorf("failed to convert row data: %v", err)
		}
		result = append(result, row)
	}

	return result, nil
}

// Count returns the total number of rows in the file
func (r *ParquetReader) Count() (int64, error) {
	return r.rowNum, nil
}

// Close closes the reader
func (r *ParquetReader) Close() error {
	if err := r.reader.Close(); err != nil {
		return err
	}
	return r.file.Close()
}

// GetSchema retrieves the schema information of the Parquet file
func (r *ParquetReader) GetSchema() (string, error) {
	schema := r.reader.Schema()
	
	// Build detailed schema information
	result := fmt.Sprintf("File contains %d rows of data\n", r.rowNum)
	result += "Schema elements (fields):\n"
	
	// Use a more friendly format to display the schema
	result += schema.String()
	
	return result, nil
}

// PrintJSON prints data in JSON format
func PrintJSON(data []map[string]interface{}, w io.Writer, pretty bool) error {
	encoder := json.NewEncoder(w)
	
	// Only set indentation if pretty is true
	if pretty {
		encoder.SetIndent("", "  ")
	}
	
	for _, row := range data {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	
	return nil
} 