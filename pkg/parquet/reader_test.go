package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

// 用于测试的简单结构体
type TestUser struct {
	Name   string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age    int32  `parquet:"name=age, type=INT32"`
	ID     int64  `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
}

// 创建测试用的parquet文件
func createTestParquetFile(t *testing.T) string {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "parquet-test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}

	filePath := filepath.Join(tempDir, "test.parquet")
	
	// 创建parquet文件
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		t.Fatalf("无法创建文件: %v", err)
	}
	defer fw.Close()

	// 创建写入器
	pw, err := writer.NewParquetWriter(fw, new(TestUser), 4)
	if err != nil {
		t.Fatalf("无法创建Parquet写入器: %v", err)
	}

	// 写入测试数据
	for i := 0; i < 100; i++ {
		user := TestUser{
			Name:   "用户" + string(rune(i+65)),
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
		}
		if err := pw.Write(user); err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		t.Fatalf("完成写入失败: %v", err)
	}

	return filePath
}

// 清理测试文件
func cleanupTestFile(filePath string) {
	os.RemoveAll(filepath.Dir(filePath))
}

func TestHead(t *testing.T) {
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// 创建读取器
	reader, err := NewParquetReader(filePath)
	if err != nil {
		t.Fatalf("无法创建读取器: %v", err)
	}
	defer reader.Close()

	// 测试读取前10行
	rows, err := reader.Head(10)
	if err != nil {
		t.Fatalf("读取前10行失败: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("期望读取10行，实际读取了%d行", len(rows))
	}

	// 测试读取前5行
	rows, err = reader.Head(5)
	if err != nil {
		t.Fatalf("读取前5行失败: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("期望读取5行，实际读取了%d行", len(rows))
	}

	// 测试读取超过文件行数
	rows, err = reader.Head(1000)
	if err != nil {
		t.Fatalf("读取全部行失败: %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("期望读取100行，实际读取了%d行", len(rows))
	}
}

func TestTail(t *testing.T) {
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// 创建读取器
	reader, err := NewParquetReader(filePath)
	if err != nil {
		t.Fatalf("无法创建读取器: %v", err)
	}
	defer reader.Close()

	// 测试读取最后10行
	rows, err := reader.Tail(10)
	if err != nil {
		t.Fatalf("读取最后10行失败: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("期望读取10行，实际读取了%d行", len(rows))
	}

	// 测试读取最后5行
	rows, err = reader.Tail(5)
	if err != nil {
		t.Fatalf("读取最后5行失败: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("期望读取5行，实际读取了%d行", len(rows))
	}

	// 测试读取超过文件行数
	rows, err = reader.Tail(1000)
	if err != nil {
		t.Fatalf("读取全部行失败: %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("期望读取100行，实际读取了%d行", len(rows))
	}
}

func TestCount(t *testing.T) {
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// 创建读取器
	reader, err := NewParquetReader(filePath)
	if err != nil {
		t.Fatalf("无法创建读取器: %v", err)
	}
	defer reader.Close()

	// 测试行数计算
	count := reader.Count()
	if count != 100 {
		t.Errorf("期望行数为100，实际为%d", count)
	}
} 