package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/parquet-go/parquet-go"
)

type ParquetReader struct {
	reader *parquet.Reader
	file   *os.File
	rowNum int64
}

func NewParquetReader(filepath string) (*ParquetReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	header := make([]byte, 4)
	_, err = file.Read(header)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read file header: %v", err)
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to reset file position: %v", err)
	}

	if string(header) != "PAR1" {
		file.Close()
		return nil, fmt.Errorf("invalid file format: the file is not a valid Parquet file")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	if fileInfo.Size() < 12 {
		file.Close()
		return nil, fmt.Errorf("file is too small to be a valid Parquet file, it might be corrupted")
	}

	var reader *parquet.Reader
	var rowNum int64

	err = func() (recErr error) {
		defer func() {
			if r := recover(); r != nil {
				recErr = fmt.Errorf("failed to create Parquet reader: %v", r)
			}
		}()
		reader = parquet.NewReader(file)
		rowNum = reader.NumRows()
		return nil
	}()

	if err != nil {
		file.Close()
		return nil, err
	}

	return &ParquetReader{
		reader: reader,
		file:   file,
		rowNum: rowNum,
	}, nil
}

func (r *ParquetReader) Head(n int) ([]map[string]interface{}, error) {
	if r == nil || r.reader == nil {
		return nil, fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	r.reader.SeekToRow(0)
	return r.readRows(n)
}

func (r *ParquetReader) Tail(n int) ([]map[string]interface{}, error) {
	if r == nil || r.reader == nil {
		return nil, fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	if r.rowNum == 0 {
		return nil, fmt.Errorf("file is empty")
	}
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	startRow := r.rowNum - int64(n)
	if startRow < 0 {
		startRow = 0
	}
	r.reader.SeekToRow(startRow)
	return r.readRows(n)
}

func (r *ParquetReader) readRows(n int) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	var err error

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic while reading rows: %v", rec)
			}
		}()

		result = make([]map[string]interface{}, 0, n)
		rowBuf := make([]parquet.Row, n)

		count, readErr := r.reader.ReadRows(rowBuf)
		if readErr != nil && readErr != io.EOF {
			err = fmt.Errorf("failed to read row data: %v", readErr)
			return
		}

		for i := 0; i < count; i++ {
			row := make(map[string]interface{})
			reconstructErr := r.reader.Schema().Reconstruct(&row, rowBuf[i])
			if reconstructErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: Skipping row %d due to error: %v\n", i, reconstructErr)
				continue
			}
			result = append(result, row)
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

// StreamAll reads all rows in batches and calls fn for each row.
// Stops early if fn returns a non-nil error.
func (r *ParquetReader) StreamAll(fn func(row map[string]interface{}) error) error {
	if r == nil || r.reader == nil {
		return fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	if r.rowNum == 0 {
		return nil
	}

	r.reader.SeekToRow(0)
	const batchSize = 256
	rowBuf := make([]parquet.Row, batchSize)

	for {
		n, readErr := r.reader.ReadRows(rowBuf)
		if n == 0 && readErr == io.EOF {
			return nil
		}
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("failed to read rows: %v", readErr)
		}

		for i := 0; i < n; i++ {
			row := make(map[string]interface{})
			if err := r.reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Skipping row due to error: %v\n", err)
				continue
			}
			if err := fn(row); err != nil {
				return err
			}
		}

		if readErr == io.EOF {
			return nil
		}
	}
}

func (r *ParquetReader) Count() (int64, error) {
	if r == nil {
		return 0, fmt.Errorf("invalid reader: reader is not initialized properly")
	}
	return r.rowNum, nil
}

func (r *ParquetReader) Close() error {
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

func (r *ParquetReader) GetSchema() (string, error) {
	if r == nil || r.reader == nil {
		return "", fmt.Errorf("invalid reader: reader is not initialized properly")
	}

	var schemaStr string
	var err error

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("error retrieving schema: %v", rec)
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

	result := fmt.Sprintf("File contains %d rows of data\n", r.rowNum)
	result += "Schema elements (fields):\n"
	result += "------------------------\n"
	result += schemaStr

	return result, nil
}

func PrintJSON(data []map[string]interface{}, w io.Writer, pretty bool) error {
	encoder := json.NewEncoder(w)
	if pretty {
		encoder.SetIndent("", "  ")
	}
	for _, row := range data {
		if err := encoder.Encode(row); err != nil {
			if pathErr, ok := err.(*os.PathError); ok && (pathErr.Err == syscall.EPIPE || pathErr.Err.Error() == "broken pipe") {
				return nil
			}
			return err
		}
	}
	return nil
}
