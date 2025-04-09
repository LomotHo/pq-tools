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

	// 检查文件是否为 Parquet 格式
	// Parquet 文件的魔术头部是 "PAR1"
	header := make([]byte, 4)
	_, err = file.Read(header)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read file header: %v", err)
	}

	// 将文件指针重置回开始位置
	_, err = file.Seek(0, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to reset file position: %v", err)
	}

	// 检查魔术头部，非 Parquet 文件给出友好错误
	if string(header) != "PAR1" {
		file.Close()
		return nil, fmt.Errorf("invalid file format: the file is not a valid Parquet file")
	}

	// Create Parquet reader
	var reader *parquet.Reader
	defer func() {
		if r := recover(); r != nil {
			file.Close()
			err = fmt.Errorf("failed to create Parquet reader: %v, the file might be corrupted or not a valid Parquet file", r)
		}
	}()
	
	reader = parquet.NewReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	
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
			// 检查是否是 broken pipe 错误，如果是则停止写入但不返回错误
			if pathErr, ok := err.(*os.PathError); ok && (pathErr.Err == syscall.EPIPE || pathErr.Err.Error() == "broken pipe") {
				return nil
			}
			return err
		}
	}
	
	return nil
} 