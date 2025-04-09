package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

// ParquetReader 是一个parquet文件的读取器
type ParquetReader struct {
	reader *parquet.Reader
	file   *os.File  // 用于关闭底层文件
	rowNum int64
}

// NewParquetReader 创建一个新的parquet文件读取器
func NewParquetReader(filepath string) (*ParquetReader, error) {
	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}

	// 创建parquet读取器
	reader := parquet.NewReader(file)
	rowNum := reader.NumRows()

	return &ParquetReader{
		reader: reader,
		file:   file,
		rowNum: rowNum,
	}, nil
}

// Head 返回文件的前n行数据
func (r *ParquetReader) Head(n int) ([]map[string]interface{}, error) {
	if r.rowNum == 0 {
		return nil, fmt.Errorf("文件为空")
	}

	// 限制行数不超过文件的总行数
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// 重置到文件开始
	if err := r.reader.SeekToRow(0); err != nil {
		return nil, fmt.Errorf("定位到文件开始失败: %v", err)
	}

	// 读取前n行
	result := make([]map[string]interface{}, 0, n)
	rowBuf := make([]parquet.Row, n)
	
	count, err := r.reader.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("读取行数据失败: %v", err)
	}
	
	// 转换到map格式
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		if err := r.reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
			return nil, fmt.Errorf("转换行数据失败: %v", err)
		}
		result = append(result, row)
	}

	return result, nil
}

// Tail 返回文件的最后n行数据
func (r *ParquetReader) Tail(n int) ([]map[string]interface{}, error) {
	if r.rowNum == 0 {
		return nil, fmt.Errorf("文件为空")
	}

	// 限制行数不超过文件的总行数
	if int64(n) > r.rowNum {
		n = int(r.rowNum)
	}

	// 确定从哪一行开始读取
	startRow := r.rowNum - int64(n)
	if startRow < 0 {
		startRow = 0
	}
	
	// 重置位置到指定行
	if err := r.reader.SeekToRow(startRow); err != nil {
		return nil, fmt.Errorf("定位到指定行失败: %v", err)
	}

	// 读取最后n行
	result := make([]map[string]interface{}, 0, n)
	rowBuf := make([]parquet.Row, n)
	
	count, err := r.reader.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("读取行数据失败: %v", err)
	}
	
	// 转换到map格式
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		if err := r.reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
			return nil, fmt.Errorf("转换行数据失败: %v", err)
		}
		result = append(result, row)
	}

	return result, nil
}

// Count 返回文件的总行数
func (r *ParquetReader) Count() (int64, error) {
	return r.rowNum, nil
}

// Close 关闭读取器
func (r *ParquetReader) Close() error {
	if err := r.reader.Close(); err != nil {
		return err
	}
	return r.file.Close()
}

// GetSchema 获取parquet文件的schema信息
func (r *ParquetReader) GetSchema() (string, error) {
	schema := r.reader.Schema()
	
	// 构建详细的schema信息
	result := fmt.Sprintf("文件包含 %d 行数据\n", r.rowNum)
	result += "Schema 元素 (字段):\n"
	
	// 使用更友好的格式展示schema
	result += schema.String()
	
	return result, nil
}

// PrintJSON 以JSON格式打印数据
func PrintJSON(data []map[string]interface{}, w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	
	for _, row := range data {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	
	return nil
} 