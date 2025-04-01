package parquet

import (
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	parquetreader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// 将接口数组转换为map数组 (写入模块专用版本)
func convertDataToMaps(data []interface{}) ([]map[string]interface{}, error) {
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

// SplitParquetFile 将parquet文件拆分成多个小文件
func SplitParquetFile(filePath string, numFiles int) error {
	// 创建reader
	reader, err := NewParquetReader(filePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	// 获取文件总行数
	totalRows := reader.Count()
	if totalRows == 0 {
		return fmt.Errorf("文件为空，无需拆分")
	}

	// 计算每个文件的行数
	rowsPerFile := int(math.Ceil(float64(totalRows) / float64(numFiles)))

	// 准备输出文件名
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = strings.TrimSuffix(baseName, ext)
	dir := filepath.Dir(filePath)

	// 创建一个新的读取器来读取所有数据
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return fmt.Errorf("无法重新打开文件: %v", err)
	}
	defer fr.Close()
	
	// 使用nil作为schema，自动适应任何parquet文件格式
	newReader, err := parquetreader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return fmt.Errorf("无法创建新的Parquet读取器: %v", err)
	}
	defer newReader.ReadStop()

	// 使用 ReadByNumber 读取所有数据
	rawData, err := newReader.ReadByNumber(int(totalRows))
	if err != nil {
		return fmt.Errorf("读取数据失败: %v", err)
	}

	// 将数据转换为 []map[string]interface{}
	allData, err := convertDataToMaps(rawData)
	if err != nil {
		return fmt.Errorf("转换数据失败: %v", err)
	}

	// 拆分数据并写入多个文件
	for i := 0; i < numFiles && i*rowsPerFile < len(allData); i++ {
		// 计算当前分片的开始和结束索引
		startIdx := i * rowsPerFile
		endIdx := (i + 1) * rowsPerFile
		if endIdx > len(allData) {
			endIdx = len(allData)
		}
		
		// 提取当前分片数据
		currentPartData := allData[startIdx:endIdx]
		
		// 创建输出文件
		outputFile := filepath.Join(dir, fmt.Sprintf("%s_%d%s", baseName, i+1, ext))
		
		// 写入分片文件
		if err := writeParquetFile(outputFile, currentPartData); err != nil {
			return fmt.Errorf("写入分片文件 %s 失败: %v", outputFile, err)
		}
	}

	return nil
}

// writeParquetFile 将数据写入parquet文件
func writeParquetFile(filePath string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("没有数据可写入")
	}

	// 创建文件
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("无法创建输出文件: %v", err)
	}
	defer fw.Close()

	// 从第一行数据构建schema
	schemaStr := "{"
	schemaStr += "\"Tag\":\"name=parquet-go-root\","
	schemaStr += "\"Fields\":["
	
	firstItem := true
	for key, val := range data[0] {
		if !firstItem {
			schemaStr += ","
		}
		firstItem = false
		
		// 根据值类型设置字段类型
		parquetType := "BYTE_ARRAY"
		convertedType := "UTF8"
		
		switch val.(type) {
		case int, int32, int64:
			parquetType = "INT64"
			convertedType = ""
		case float32, float64:
			parquetType = "DOUBLE"
			convertedType = ""
		case bool:
			parquetType = "BOOLEAN"
			convertedType = ""
		}
		
		fieldSchema := fmt.Sprintf("{\"Tag\":\"name=%s, type=%s", key, parquetType)
		if convertedType != "" {
			fieldSchema += fmt.Sprintf(", convertedtype=%s", convertedType)
		}
		fieldSchema += "\"}"
		
		schemaStr += fieldSchema
	}
	
	schemaStr += "]}"

	// 创建JSON写入器
	pw, err := writer.NewJSONWriter(schemaStr, fw, 4)
	if err != nil {
		return fmt.Errorf("无法创建Parquet写入器: %v", err)
	}
	defer pw.WriteStop()

	// 写入数据
	for _, row := range data {
		// 转换为JSON字符串
		jsonData, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("序列化行数据失败: %v", err)
		}
		
		if err := pw.Write(string(jsonData)); err != nil {
			return fmt.Errorf("写入行数据失败: %v", err)
		}
	}

	return nil
} 