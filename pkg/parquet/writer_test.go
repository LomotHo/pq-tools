package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSplitParquetFile(t *testing.T) {
	t.Run("flat/split into 2", func(t *testing.T) {
		src := fixture("flat.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "flat.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 2)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "flat", ".parquet", 2)
		if total != 100 {
			t.Errorf("total rows should be 100, got %d", total)
		}
	})

	t.Run("flat/split into 3", func(t *testing.T) {
		src := fixture("flat.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "flat.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 3)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "flat", ".parquet", 3)
		if total != 100 {
			t.Errorf("total rows should be 100, got %d", total)
		}
	})

	t.Run("flat/split into 1", func(t *testing.T) {
		src := fixture("flat.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "flat.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 1)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "flat", ".parquet", 1)
		if total != 100 {
			t.Errorf("total rows should be 100, got %d", total)
		}
	})

	t.Run("flat/more splits than rows", func(t *testing.T) {
		src := fixture("flat.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "flat.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 150)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := int64(0)
		for i := 1; i <= 150; i++ {
			f := filepath.Join(dir, "flat_"+itoa(i)+".parquet")
			if _, err := os.Stat(f); os.IsNotExist(err) {
				continue
			}
			r, err := NewParquetReader(f)
			if err != nil {
				t.Fatalf("failed to read split file %d: %v", i, err)
			}
			c, _ := r.Count()
			total += c
			r.Close()
		}
		if total != 100 {
			t.Errorf("total rows should be 100, got %d", total)
		}
	})

	t.Run("nested struct split", func(t *testing.T) {
		src := fixture("nested_struct.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "nested_struct.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 2)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "nested_struct", ".parquet", 2)
		if total != 50 {
			t.Errorf("total rows should be 50, got %d", total)
		}

		r, err := NewParquetReader(filepath.Join(dir, "nested_struct_1.parquet"))
		if err != nil {
			t.Fatalf("failed to read split file: %v", err)
		}
		defer r.Close()
		rows, err := r.Head(1)
		if err != nil {
			t.Fatalf("failed to read rows: %v", err)
		}
		if _, ok := rows[0]["info"]; !ok {
			t.Error("split file should preserve nested struct 'info'")
		}
	})

	t.Run("list struct split", func(t *testing.T) {
		src := fixture("list_struct.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "list_struct.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 2)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "list_struct", ".parquet", 2)
		if total != 50 {
			t.Errorf("total rows should be 50, got %d", total)
		}
	})

	t.Run("map simple split", func(t *testing.T) {
		src := fixture("map_simple.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "map_simple.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 2)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "map_simple", ".parquet", 2)
		if total != 50 {
			t.Errorf("total rows should be 50, got %d", total)
		}
	})

	t.Run("deeply nested split", func(t *testing.T) {
		src := fixture("deeply_nested.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "deeply_nested.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 2)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "deeply_nested", ".parquet", 2)
		if total != 20 {
			t.Errorf("total rows should be 20, got %d", total)
		}
	})

	t.Run("multi rowgroup split", func(t *testing.T) {
		src := fixture("multi_rowgroup.parquet")
		dir := t.TempDir()
		tmp := filepath.Join(dir, "multi_rowgroup.parquet")
		copyFile(t, src, tmp)

		err := SplitParquetFile(tmp, 3)
		if err != nil {
			t.Fatalf("split failed: %v", err)
		}

		total := verifySplitFiles(t, dir, "multi_rowgroup", ".parquet", 3)
		if total != 90 {
			t.Errorf("total rows should be 90, got %d", total)
		}
	})
}

// --- helpers ---

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("failed to read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		t.Fatalf("failed to write %s: %v", dst, err)
	}
}

func verifySplitFiles(t *testing.T, dir, base, ext string, n int) int64 {
	t.Helper()
	total := int64(0)
	found := 0
	for i := 1; i <= n; i++ {
		f := filepath.Join(dir, base+"_"+itoa(i)+ext)
		if _, err := os.Stat(f); os.IsNotExist(err) {
			continue
		}
		found++
		r, err := NewParquetReader(f)
		if err != nil {
			t.Fatalf("failed to read split file %d: %v", i, err)
		}
		c, _ := r.Count()
		if c == 0 {
			t.Errorf("split file %d has 0 rows", i)
		}
		total += c
		r.Close()
	}
	if found == 0 {
		t.Fatal("no split files found")
	}
	return total
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
