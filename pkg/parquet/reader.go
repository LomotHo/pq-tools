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
	genericReader any      // Use dynamic type to hold either *parquet.GenericReader[any] or *parquet.Reader
	file          *os.File // Used to close the underlying file
	rowNum        int64
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

	// Try to create reader with error recovery
	var rowNum int64
	var genericReader any

	err = func() (recErr error) {
		defer func() {
			if r := recover(); r != nil {
				recErr = fmt.Errorf("failed to create Parquet reader: %v", r)
			}
		}()

		// Try to use the more type-safe GenericReader
		reader := parquet.NewGenericReader[map[string]interface{}](file)
		rowNum = reader.NumRows()
		genericReader = reader
		return nil
	}()

	// If GenericReader fails, try the classic Reader as fallback
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: %v, falling back to classic Reader\n", err)

		err = func() (recErr error) {
			defer func() {
				if r := recover(); r != nil {
					recErr = fmt.Errorf("failed to create classic Parquet reader: %v", r)
				}
			}()

			// Reset file position
			_, seekErr := file.Seek(0, 0)
			if seekErr != nil {
				return seekErr
			}

			reader := parquet.NewReader(file)
			rowNum = reader.NumRows()
			genericReader = reader
			return nil
		}()

		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to create any Parquet reader: %v", err)
		}
	}

	return &ParquetReader{
		genericReader: genericReader,
		file:          file,
		rowNum:        rowNum,
	}, nil
}

// Head returns the first n rows of the file
func (r *ParquetReader) Head(n int) ([]map[string]interface{}, error) {
	// Defensive programming: check if reader is initialized
	if r == nil || r.genericReader == nil {
		return nil, fmt.Errorf("invalid reader: reader is not initialized properly")
	}

	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Limit the number of rows to not exceed the total rows in the file
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// Use recovery block to handle potential panics
	var result []map[string]interface{}
	var err error

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic while reading rows: %v", rec)
			}
		}()

		// Handle different reader types
		switch reader := r.genericReader.(type) {
		case *parquet.GenericReader[map[string]interface{}]:
			// Seek to beginning
			reader.SeekToRow(0)

			// Read rows with GenericReader
			rows := make([]map[string]interface{}, n)
			count, readErr := reader.Read(rows)
			if readErr != nil && readErr != io.EOF {
				err = fmt.Errorf("failed to read row data: %v", readErr)
				return
			}

			// Take only the rows we actually read
			result = rows[:count]

		case *parquet.Reader:
			// Seek to beginning
			reader.SeekToRow(0)

			// Read rows with classic Reader
			result = make([]map[string]interface{}, 0, n)
			rowBuf := make([]parquet.Row, n)

			// Read a batch of rows
			count, readErr := reader.ReadRows(rowBuf)
			if readErr != nil && readErr != io.EOF {
				err = fmt.Errorf("failed to read row data: %v", readErr)
				return
			}

			// Process each row
			for i := 0; i < count; i++ {
				row := make(map[string]interface{})

				// Safe reconstruction with error handling
				reconstructErr := func() error {
					defer func() {
						if rec := recover(); rec != nil {
							err = fmt.Errorf("panic during row reconstruction: %v", rec)
						}
					}()
					return reader.Schema().Reconstruct(&row, rowBuf[i])
				}()

				if reconstructErr != nil {
					// Skip rows that can't be reconstructed
					fmt.Fprintf(os.Stderr, "Warning: Skipping row %d due to error: %v\n", i, reconstructErr)
					continue
				}

				result = append(result, row)
			}

		default:
			err = fmt.Errorf("unknown reader type: %T", reader)
		}
	}()

	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no rows could be read from the file")
	}

	return result, nil
}

// Tail returns the last n rows of the file
func (r *ParquetReader) Tail(n int) ([]map[string]interface{}, error) {
	// Defensive programming: check if reader is initialized
	if r == nil || r.genericReader == nil {
		return nil, fmt.Errorf("invalid reader: reader is not initialized properly")
	}

	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Limit the number of rows to not exceed the total rows in the file
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// Determine the starting row
	startRow := r.rowNum - int64(n)
	if startRow < 0 {
		startRow = 0
	}

	// Use recovery block to handle potential panics
	var result []map[string]interface{}
	var err error

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic while reading rows: %v", rec)
			}
		}()

		// Handle different reader types
		switch reader := r.genericReader.(type) {
		case *parquet.GenericReader[map[string]interface{}]:
			// Seek to the starting row
			reader.SeekToRow(startRow)

			// Read rows with GenericReader
			rows := make([]map[string]interface{}, n)
			count, readErr := reader.Read(rows)
			if readErr != nil && readErr != io.EOF {
				err = fmt.Errorf("failed to read row data: %v", readErr)
				return
			}

			// Take only the rows we actually read
			result = rows[:count]

		case *parquet.Reader:
			// Seek to the starting row
			reader.SeekToRow(startRow)

			// Read rows with classic Reader
			result = make([]map[string]interface{}, 0, n)
			rowBuf := make([]parquet.Row, n)

			// Read a batch of rows
			count, readErr := reader.ReadRows(rowBuf)
			if readErr != nil && readErr != io.EOF {
				err = fmt.Errorf("failed to read row data: %v", readErr)
				return
			}

			// Process each row
			for i := 0; i < count; i++ {
				row := make(map[string]interface{})

				// Safe reconstruction with error handling
				reconstructErr := func() error {
					defer func() {
						if rec := recover(); rec != nil {
							err = fmt.Errorf("panic during row reconstruction: %v", rec)
						}
					}()
					return reader.Schema().Reconstruct(&row, rowBuf[i])
				}()

				if reconstructErr != nil {
					// Skip rows that can't be reconstructed
					fmt.Fprintf(os.Stderr, "Warning: Skipping row %d due to error: %v\n", i, reconstructErr)
					continue
				}

				result = append(result, row)
			}

		default:
			err = fmt.Errorf("unknown reader type: %T", reader)
		}
	}()

	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no rows could be read from the file")
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

	// Close the reader based on its type
	switch reader := r.genericReader.(type) {
	case *parquet.GenericReader[map[string]interface{}]:
		err = reader.Close()
	case *parquet.Reader:
		err = reader.Close()
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
	if r == nil || r.genericReader == nil {
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

		var schema *parquet.Schema

		// Get schema based on reader type
		switch reader := r.genericReader.(type) {
		case *parquet.GenericReader[map[string]interface{}]:
			schema = reader.Schema()
		case *parquet.Reader:
			schema = reader.Schema()
		default:
			err = fmt.Errorf("unknown reader type: %T", r.genericReader)
			return
		}

		if schema != nil {
			schemaStr = schema.String()
		} else {
			err = fmt.Errorf("failed to get schema: schema is nil")
		}
	}()

	if err != nil {
		return "", err
	}

	// Build detailed schema information with improved formatting
	result := fmt.Sprintf("File contains %d rows of data\n", r.rowNum)
	result += "Schema elements (fields):\n"
	result += "------------------------\n" // Add separator line
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
