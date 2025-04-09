package parquet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSplitParquetFile(t *testing.T) {
	// 创建测试文件
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// 拆分成2个文件
	err := SplitParquetFile(filePath, 2)
	if err != nil {
		t.Fatalf("拆分文件失败: %v", err)
	}

	// 检查生成的文件
	dir := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = baseName[:len(baseName)-len(ext)]

	// 验证第一个分割文件
	file1 := filepath.Join(dir, baseName+"_1"+ext)
	
	// 使用我们的ParquetReader来检查文件
	pr1, err := NewParquetReader(file1)
	if err != nil {
		t.Fatalf("无法创建分割文件1的读取器: %v", err)
	}
	defer pr1.Close()
	
	// 检查行数，应该是50行（总共100行分成2份）
	count1, err := pr1.Count()
	if err != nil {
		t.Fatalf("获取分割文件1的行数失败: %v", err)
	}
	if count1 != 50 {
		t.Errorf("期望第一个文件有50行数据，实际有%d行", count1)
	}

	// 验证第二个分割文件
	file2 := filepath.Join(dir, baseName+"_2"+ext)
	
	pr2, err := NewParquetReader(file2)
	if err != nil {
		t.Fatalf("无法创建分割文件2的读取器: %v", err)
	}
	defer pr2.Close()
	
	// 检查行数，应该是50行（总共100行分成2份）
	count2, err := pr2.Count()
	if err != nil {
		t.Fatalf("获取分割文件2的行数失败: %v", err)
	}
	if count2 != 50 {
		t.Errorf("期望第二个文件有50行数据，实际有%d行", count2)
	}

	// 测试拆分成3个文件
	err = SplitParquetFile(filePath, 3)
	if err != nil {
		t.Fatalf("拆分文件为3份失败: %v", err)
	}

	// 验证第一个分割文件（3份）
	file1 = filepath.Join(dir, baseName+"_1"+ext)
	
	pr1, err = NewParquetReader(file1)
	if err != nil {
		t.Fatalf("无法创建分割文件1的读取器: %v", err)
	}
	defer pr1.Close()
	
	// 检查行数，应该是33或34行左右（总共100行分成3份）
	count1, err = pr1.Count()
	if err != nil {
		t.Fatalf("获取分割文件1的行数失败: %v", err)
	}
	expectedRows := int64(34)
	if count1 != expectedRows {
		t.Errorf("期望第一个文件有%d行数据，实际有%d行", expectedRows, count1)
	}

	// 验证文件总行数是否匹配原始文件
	totalRows := int64(0)
	for i := 1; i <= 3; i++ {
		fileN := filepath.Join(dir, baseName+"_"+string(rune(i+48))+ext)
		if _, err := os.Stat(fileN); os.IsNotExist(err) {
			t.Logf("文件%d不存在，跳过", i)
			continue
		}
		
		prN, err := NewParquetReader(fileN)
		if err != nil {
			t.Fatalf("无法创建分割文件%d的读取器: %v", i, err)
		}
		defer prN.Close()
		
		countN, err := prN.Count()
		if err != nil {
			t.Fatalf("获取分割文件%d的行数失败: %v", i, err)
		}
		totalRows += countN
	}
	
	if totalRows != 100 {
		t.Errorf("期望总行数为100，实际为%d", totalRows)
	}
} 