package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// ParquetReader 是parquet文件读取器
type ParquetReader struct {
	Reader *reader.ParquetReader
	file   source.ParquetFile
	path   string // 保存文件路径
}

// NewParquetReader 创建一个新的parquet读取器
func NewParquetReader(filePath string) (*ParquetReader, error) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %v", err)
	}

	// 使用nil作为schema，这样parquet-go会自动读取文件中的schema
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		fr.Close()
		return nil, fmt.Errorf("无法创建Parquet读取器: %v", err)
	}

	return &ParquetReader{
		Reader: pr,
		file:   fr,
		path:   filePath,
	}, nil
}

// Close 关闭读取器
func (r *ParquetReader) Close() error {
	r.Reader.ReadStop()
	return r.file.Close()
}

// Head 读取parquet文件前n行
func (r *ParquetReader) Head(n int) ([]map[string]interface{}, error) {
	if n <= 0 {
		n = 10 // 默认显示10行
	}

	// 获取文件总行数
	numRows := r.Reader.GetNumRows()
	if numRows == 0 {
		return []map[string]interface{}{}, nil
	}
	
	if n > int(numRows) {
		n = int(numRows)
	}

	// 创建一个新的读取器实例，确保从文件开始读取
	fr, err := local.NewLocalFileReader(r.path)
	if err != nil {
		return nil, fmt.Errorf("无法重新打开文件: %v", err)
	}
	defer fr.Close()
	
	// 使用nil作为schema，自动适应任何parquet文件格式
	newReader, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, fmt.Errorf("无法创建新的Parquet读取器: %v", err)
	}
	defer newReader.ReadStop()

	// 读取数据
	data, err := newReader.ReadByNumber(int(n))
	if err != nil {
		return nil, fmt.Errorf("读取数据失败: %v", err)
	}

	// 将结果转换为map[string]interface{}
	return convertToMaps(data)
}

// Tail 读取parquet文件后n行
func (r *ParquetReader) Tail(n int) ([]map[string]interface{}, error) {
	if n <= 0 {
		n = 10 // 默认显示10行
	}

	// 获取文件总行数
	numRows := r.Reader.GetNumRows()
	if numRows == 0 {
		return []map[string]interface{}{}, nil
	}
	
	if n > int(numRows) {
		n = int(numRows)
	}

	// 创建一个新的读取器实例
	fr, err := local.NewLocalFileReader(r.path)
	if err != nil {
		return nil, fmt.Errorf("无法重新打开文件: %v", err)
	}
	defer fr.Close()
	
	// 使用nil作为schema，自动适应任何parquet文件格式
	newReader, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, fmt.Errorf("无法创建新的Parquet读取器: %v", err)
	}
	defer newReader.ReadStop()
	
	// 读取所有数据
	data, err := newReader.ReadByNumber(int(numRows))
	if err != nil {
		return nil, fmt.Errorf("读取数据失败: %v", err)
	}
	
	// 将结果转换为map[string]interface{}
	allRows, err := convertToMaps(data)
	if err != nil {
		return nil, err
	}
	
	// 取最后n行
	startIdx := len(allRows) - n
	if startIdx < 0 {
		startIdx = 0
	}
	
	return allRows[startIdx:], nil
}

// 将接口数组转换为map数组
func convertToMaps(data []interface{}) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, len(data))
	
	for i, item := range data {
		// 先将数据序列化为JSON，再反序列化为map
		jsonBytes, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("序列化数据失败: %v", err)
		}
		
		var mapData map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &mapData); err != nil {
			return nil, fmt.Errorf("反序列化数据失败: %v", err)
		}
		
		// 创建新的map，将所有键转换为小写
		normalizedMap := make(map[string]interface{})
		for k, v := range mapData {
			lowerKey := strings.ToLower(k)
			normalizedMap[lowerKey] = v
		}
		
		result[i] = normalizedMap
	}
	
	return result, nil
}

// Count 返回parquet文件的行数
func (r *ParquetReader) Count() int64 {
	return r.Reader.GetNumRows()
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