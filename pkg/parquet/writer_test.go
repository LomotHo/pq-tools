package parquet

import (
	"path/filepath"
	"testing"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
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
	fr1, err := local.NewLocalFileReader(file1)
	if err != nil {
		t.Fatalf("无法打开分割文件1: %v", err)
	}
	defer fr1.Close()
	
	pr1, err := reader.NewParquetReader(fr1, nil, 4)
	if err != nil {
		t.Fatalf("无法创建分割文件1的读取器: %v", err)
	}
	defer pr1.ReadStop()
	
	// 检查行数，应该是50行（总共100行分成2份）
	if pr1.GetNumRows() != 50 {
		t.Errorf("期望第一个文件有50行数据，实际有%d行", pr1.GetNumRows())
	}

	// 验证第二个分割文件
	file2 := filepath.Join(dir, baseName+"_2"+ext)
	fr2, err := local.NewLocalFileReader(file2)
	if err != nil {
		t.Fatalf("无法打开分割文件2: %v", err)
	}
	defer fr2.Close()
	
	pr2, err := reader.NewParquetReader(fr2, nil, 4)
	if err != nil {
		t.Fatalf("无法创建分割文件2的读取器: %v", err)
	}
	defer pr2.ReadStop()
	
	// 检查行数，应该是50行（总共100行分成2份）
	if pr2.GetNumRows() != 50 {
		t.Errorf("期望第二个文件有50行数据，实际有%d行", pr2.GetNumRows())
	}

	// 测试拆分成3个文件
	err = SplitParquetFile(filePath, 3)
	if err != nil {
		t.Fatalf("拆分文件为3份失败: %v", err)
	}

	// 验证第一个分割文件（3份）
	file1 = filepath.Join(dir, baseName+"_1"+ext)
	fr1, err = local.NewLocalFileReader(file1)
	if err != nil {
		t.Fatalf("无法打开分割文件1: %v", err)
	}
	defer fr1.Close()
	
	pr1, err = reader.NewParquetReader(fr1, nil, 4)
	if err != nil {
		t.Fatalf("无法创建分割文件1的读取器: %v", err)
	}
	defer pr1.ReadStop()
	
	// 检查行数，应该是34行左右（总共100行分成3份）
	expectedRows := int64(34)
	if pr1.GetNumRows() != expectedRows {
		t.Errorf("期望第一个文件有%d行数据，实际有%d行", expectedRows, pr1.GetNumRows())
	}

	// 验证文件总行数是否匹配原始文件
	totalRows := int64(0)
	for i := 1; i <= 3; i++ {
		fileN := filepath.Join(dir, baseName+"_"+string(rune(i+48))+ext)
		frN, err := local.NewLocalFileReader(fileN)
		if err != nil {
			t.Logf("文件%d不存在，跳过: %v", i, err)
			continue
		}
		defer frN.Close()
		
		prN, err := reader.NewParquetReader(frN, nil, 4)
		if err != nil {
			t.Fatalf("无法创建分割文件%d的读取器: %v", i, err)
		}
		defer prN.ReadStop()
		
		totalRows += prN.GetNumRows()
	}
	
	if totalRows != 100 {
		t.Errorf("期望总行数为100，实际为%d", totalRows)
	}
} 