package parquet

import (
	"os"
	"testing"
)

const externalTestFile = "/minimax-dialogue/airflow/data_pipeline/pretrain/cc_pipeline/domain_bucket/kaang_data_20260225/text_with_image/ml/general_output/text_bucket_EHQ_mm_bucket_HQ/000_00994.parquet"

func TestExternalComplexFile(t *testing.T) {
	if _, err := os.Stat(externalTestFile); os.IsNotExist(err) {
		t.Skip("external test file not available")
	}

	t.Run("open and count", func(t *testing.T) {
		r, err := NewParquetReader(externalTestFile)
		if err != nil {
			t.Fatalf("failed to open: %v", err)
		}
		defer r.Close()

		count, err := r.Count()
		if err != nil {
			t.Fatalf("count error: %v", err)
		}
		if count != 40 {
			t.Errorf("expected 40 rows, got %d", count)
		}
	})

	t.Run("head", func(t *testing.T) {
		r, err := NewParquetReader(externalTestFile)
		if err != nil {
			t.Fatalf("failed to open: %v", err)
		}
		defer r.Close()

		rows, err := r.Head(5)
		if err != nil {
			t.Fatalf("head error: %v", err)
		}
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
		if _, ok := rows[0]["id"]; !ok {
			t.Error("missing 'id' field")
		}
		if _, ok := rows[0]["data"]; !ok {
			t.Error("missing 'data' field")
		}
	})

	t.Run("schema", func(t *testing.T) {
		r, err := NewParquetReader(externalTestFile)
		if err != nil {
			t.Fatalf("failed to open: %v", err)
		}
		defer r.Close()

		schema, err := r.GetSchema()
		if err != nil {
			t.Fatalf("schema error: %v", err)
		}
		if schema == "" {
			t.Error("schema should not be empty")
		}
	})
}
