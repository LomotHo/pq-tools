package parquet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSplitParquetFile(t *testing.T) {
	// Create test file
	filePath := createTestParquetFile(t)
	defer cleanupTestFile(filePath)

	// Split into 2 files
	err := SplitParquetFile(filePath, 2)
	if err != nil {
		t.Fatalf("Failed to split file: %v", err)
	}

	// Check generated files
	dir := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	baseName = baseName[:len(baseName)-len(ext)]

	// Verify the first split file
	file1 := filepath.Join(dir, baseName+"_1"+ext)
	
	// Use our ParquetReader to check the file
	pr1, err := NewParquetReader(file1)
	if err != nil {
		t.Fatalf("Failed to create reader for split file 1: %v", err)
	}
	defer pr1.Close()
	
	// Check row count, should be 50 rows (total 100 rows split into 2 parts)
	count1, err := pr1.Count()
	if err != nil {
		t.Fatalf("Failed to get row count for split file 1: %v", err)
	}
	if count1 != 50 {
		t.Errorf("Expected the first file to have 50 rows, actual count is %d", count1)
	}

	// Verify the second split file
	file2 := filepath.Join(dir, baseName+"_2"+ext)
	
	pr2, err := NewParquetReader(file2)
	if err != nil {
		t.Fatalf("Failed to create reader for split file 2: %v", err)
	}
	defer pr2.Close()
	
	// Check row count, should be 50 rows (total 100 rows split into 2 parts)
	count2, err := pr2.Count()
	if err != nil {
		t.Fatalf("Failed to get row count for split file 2: %v", err)
	}
	if count2 != 50 {
		t.Errorf("Expected the second file to have 50 rows, actual count is %d", count2)
	}

	// Test splitting into 3 files
	err = SplitParquetFile(filePath, 3)
	if err != nil {
		t.Fatalf("Failed to split file into 3 parts: %v", err)
	}

	// Verify the first split file (3 parts)
	file1 = filepath.Join(dir, baseName+"_1"+ext)
	
	pr1, err = NewParquetReader(file1)
	if err != nil {
		t.Fatalf("Failed to create reader for split file 1: %v", err)
	}
	defer pr1.Close()
	
	// Check row count, should be around 33 or 34 rows (total 100 rows split into 3 parts)
	count1, err = pr1.Count()
	if err != nil {
		t.Fatalf("Failed to get row count for split file 1: %v", err)
	}
	expectedRows := int64(34)
	if count1 != expectedRows {
		t.Errorf("Expected the first file to have %d rows, actual count is %d", expectedRows, count1)
	}

	// Verify that the total row count matches the original file
	totalRows := int64(0)
	for i := 1; i <= 3; i++ {
		fileN := filepath.Join(dir, baseName+"_"+string(rune(i+48))+ext)
		if _, err := os.Stat(fileN); os.IsNotExist(err) {
			t.Logf("File %d does not exist, skipping", i)
			continue
		}
		
		prN, err := NewParquetReader(fileN)
		if err != nil {
			t.Fatalf("Failed to create reader for split file %d: %v", i, err)
		}
		defer prN.Close()
		
		countN, err := prN.Count()
		if err != nil {
			t.Fatalf("Failed to get row count for split file %d: %v", i, err)
		}
		totalRows += countN
	}
	
	if totalRows != 100 {
		t.Errorf("Expected total row count to be 100, actual count is %d", totalRows)
	}
} 