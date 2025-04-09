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

// SplitParquetFile 将parquet文件拆分成多个小文件
func SplitParquetFile(filePath string, numFiles int) error {
	// 打开原始文件
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("无法打开文件: %v", err)
	}
	defer f.Close()

	// 解析parquet文件
	reader := parquet.NewReader(f)
	defer reader.Close()

	// 获取文件总行数
	totalRows := reader.NumRows()
	if totalRows == 0 {
		return fmt.Errorf("文件为空，无需拆分")
	}

	// 计算每个文件的行数
	rowsPerFile := int64(math.Ceil(float64(totalRows) / float64(numFiles)))

	// 准备输出文件名
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = strings.TrimSuffix(baseName, ext)
	dir := filepath.Dir(filePath)

	// 获取Schema
	schema := reader.Schema()

	// 读取所有数据
	allRows := make([]map[string]interface{}, totalRows)
	rowBuf := make([]parquet.Row, totalRows)
	
	// 重置位置到开始
	if err := reader.SeekToRow(0); err != nil {
		return fmt.Errorf("定位到开始行失败: %v", err)
	}
	
	// 读取所有行
	count, err := reader.ReadRows(rowBuf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("读取数据失败: %v", err)
	}
	
	// 转换到map格式
	for i := 0; i < count; i++ {
		row := make(map[string]interface{})
		if err := reader.Schema().Reconstruct(&row, rowBuf[i]); err != nil {
			return fmt.Errorf("转换行数据失败: %v", err)
		}
		allRows[i] = row
	}
	
	// 拆分数据到多个文件
	for i := 0; i < numFiles; i++ {
		startRow := int64(i) * rowsPerFile
		
		// 如果起始行超过文件总行数，退出循环
		if startRow >= int64(count) {
			break
		}
		
		// 计算当前分片应该包含的行数
		endRow := startRow + rowsPerFile
		if endRow > int64(count) {
			endRow = int64(count)
		}
		
		// 创建输出文件
		outputPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", baseName, i+1, ext))
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("无法创建输出文件 %s: %v", outputPath, err)
		}
		
		// 创建parquet写入器
		writer := parquet.NewWriter(outputFile, schema)
		
		// 写入指定数量的行
		for j := startRow; j < endRow; j++ {
			// 写入一行数据
			if err := writer.Write(allRows[j]); err != nil {
				outputFile.Close()
				return fmt.Errorf("写入行失败: %v", err)
			}
		}
		
		// 完成写入并关闭文件
		if err := writer.Close(); err != nil {
			outputFile.Close()
			return fmt.Errorf("关闭写入器失败: %v", err)
		}
		
		if err := outputFile.Close(); err != nil {
			return fmt.Errorf("关闭输出文件失败: %v", err)
		}
	}

	return nil
} 