package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"syscall"

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

	// Check if the file is in Parquet format
	// Parquet files have "PAR1" magic header
	header := make([]byte, 4)
	_, err = file.Read(header)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read file header: %v", err)
	}

	// Reset file pointer to beginning
	_, err = file.Seek(0, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to reset file position: %v", err)
	}

	// Check magic header, provide friendly error for non-Parquet files
	if string(header) != "PAR1" {
		file.Close()
		return nil, fmt.Errorf("invalid file format: the file is not a valid Parquet file")
	}

	// Check file size to ensure integrity
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}
	
	// File is too small to be a valid Parquet file
	if fileInfo.Size() < 12 { // PAR1 + footer + PAR1
		file.Close()
		return nil, fmt.Errorf("file is too small to be a valid Parquet file, it might be corrupted")
	}
	
	// Create Parquet reader
	var reader *parquet.Reader
	var readErr error
	
	// Use defer/recover to catch all possible panics
	defer func() {
		if r := recover(); r != nil {
			file.Close()
			err = fmt.Errorf("failed to create Parquet reader: the file might be corrupted or not a valid Parquet file")
		}
	}()
	
	reader = parquet.NewReader(file)
	if readErr != nil {
		file.Close()
		return nil, fmt.Errorf("error reading Parquet file: the file might be corrupted")
	}
	
	// Check if reader is nil
	if reader == nil {
		file.Close()
		return nil, fmt.Errorf("failed to create Parquet reader: the file might be corrupted")
	}
	
	// Safely get row count
	var rowNum int64
	rowNumErr := func() error {
		defer func() {
			if r := recover(); r != nil {
				rowNum = 0
				return
			}
		}()
		rowNum = reader.NumRows()
		return nil
	}()
	
	if rowNumErr != nil {
		reader.Close()
		file.Close()
		return nil, fmt.Errorf("failed to read row count: the file might be corrupted")
	}

	return &ParquetReader{
		reader: reader,
		file:   file,
		rowNum: rowNum,
	}, nil
}

// Head returns the first n rows of the file
func (r *ParquetReader) Head(n int) ([]map[string]interface{}, error) {
	// Defensive programming: check if r and r.reader are nil
	if r == nil || r.reader == nil {
		return nil, fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	
	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Limit the number of rows to not exceed the total rows in the file
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// Use recover to catch potential panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Error reading Parquet file: %v\n", r)
		}
	}()

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
	// Defensive programming: check if r and r.reader are nil
	if r == nil || r.reader == nil {
		return nil, fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	
	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Limit the number of rows to not exceed the total rows in the file
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// Use recover to catch potential panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Error reading Parquet file: %v\n", r)
		}
	}()

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
	// Defensive programming: check if r is nil
	if r == nil {
		return 0, fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	return r.rowNum, nil
}

// Close closes the reader
func (r *ParquetReader) Close() error {
	// Defensive programming: check if r is nil
	if r == nil {
		return nil
	}
	
	var err error
	if r.reader != nil {
		err = r.reader.Close()
	}
	
	if r.file != nil {
		if fileErr := r.file.Close(); fileErr != nil && err == nil {
			err = fileErr
		}
	}
	
	return err
}

// GetSchema retrieves the schema information of the Parquet file
func (r *ParquetReader) GetSchema() (string, error) {
	// Defensive programming: check if r and r.reader are nil
	if r == nil || r.reader == nil {
		return "", fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	
	// Use recover to catch potential panics
	var schemaStr string
	var err error
	
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("error retrieving schema: %v", r)
			}
		}()
		schema := r.reader.Schema()
		if schema != nil {
			schemaStr = schema.String()
		} else {
			err = fmt.Errorf("failed to get schema: schema is nil")
		}
	}()
	
	if err != nil {
		return "", err
	}
	
	// Build detailed schema information
	result := fmt.Sprintf("File contains %d rows of data\n", r.rowNum)
	result += "Schema elements (fields):\n"
	result += schemaStr
	
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
			// Check for broken pipe error, stop writing but don't return error
			if pathErr, ok := err.(*os.PathError); ok && (pathErr.Err == syscall.EPIPE || pathErr.Err.Error() == "broken pipe") {
				return nil
			}
			return err
		}
	}
	
	return nil
} 