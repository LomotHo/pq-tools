package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestUser is a simple struct for testing
type TestUser struct {
	Name   string  `parquet:"name=name"`
	Age    int32   `parquet:"name=age"`
	ID     int64   `parquet:"name=id"`
	Weight float32 `parquet:"name=weight"`
	Active bool    `parquet:"name=active"`
}

// createTestParquetFile creates a test Parquet file
func createTestParquetFile(t *testing.T) string {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "parquet-test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}

	filePath := filepath.Join(tempDir, "test.parquet")
	
	// Create Parquet file
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Create writer
	writer := parquet.NewWriter(file)

	// Write test data
	for i := 0; i < 100; i++ {
		user := TestUser{
			Name:   "User" + string(rune(i+65)),
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Active: i%2 == 0,
		}
		if err := writer.Write(&user); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	return filePath
}

// cleanupTestFile cleans up the test file
func cleanupTestFile(filePath string) {
	os.RemoveAll(filepath.Dir(filePath))
}

func TestHead(t *testing.T) {
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// Create reader
	reader, err := NewParquetReader(filePath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test reading the first 10 rows
	rows, err := reader.Head(10)
	if err != nil {
		t.Fatalf("Failed to read the first 10 rows: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("Expected to read 10 rows, actually read %d rows", len(rows))
	}

	// Test reading the first 5 rows
	rows, err = reader.Head(5)
	if err != nil {
		t.Fatalf("Failed to read the first 5 rows: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("Expected to read 5 rows, actually read %d rows", len(rows))
	}

	// Test reading more rows than the file has
	rows, err = reader.Head(1000)
	if err != nil {
		t.Fatalf("Failed to read all rows: %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("Expected to read 100 rows, actually read %d rows", len(rows))
	}
}

func TestTail(t *testing.T) {
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// Create reader
	reader, err := NewParquetReader(filePath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test reading the last 10 rows
	rows, err := reader.Tail(10)
	if err != nil {
		t.Fatalf("Failed to read the last 10 rows: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("Expected to read 10 rows, actually read %d rows", len(rows))
	}

	// Test reading the last 5 rows
	rows, err = reader.Tail(5)
	if err != nil {
		t.Fatalf("Failed to read the last 5 rows: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("Expected to read 5 rows, actually read %d rows", len(rows))
	}

	// Test reading more rows than the file has
	rows, err = reader.Tail(1000)
	if err != nil {
		t.Fatalf("Failed to read all rows: %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("Expected to read 100 rows, actually read %d rows", len(rows))
	}
}

func TestCount(t *testing.T) {
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// Create reader
	reader, err := NewParquetReader(filePath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test row count calculation
	count, err := reader.Count()
	if err != nil {
		t.Fatalf("Failed to get row count: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected row count to be 100, actual count is %d", count)
	}
} 