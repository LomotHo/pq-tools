package parquet

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func fixtureDir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "testdata")
}

func fixture(name string) string {
	return filepath.Join(fixtureDir(), name)
}

// --- NewParquetReader ---

func TestNewParquetReader(t *testing.T) {
	t.Run("flat schema", func(t *testing.T) {
		r, err := NewParquetReader(fixture("flat.parquet"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		if r.rowNum != 100 {
			t.Errorf("expected 100 rows, got %d", r.rowNum)
		}
	})

	t.Run("nested struct", func(t *testing.T) {
		r, err := NewParquetReader(fixture("nested_struct.parquet"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		if r.rowNum != 50 {
			t.Errorf("expected 50 rows, got %d", r.rowNum)
		}
	})

	t.Run("map types", func(t *testing.T) {
		r, err := NewParquetReader(fixture("map_simple.parquet"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		if r.rowNum != 50 {
			t.Errorf("expected 50 rows, got %d", r.rowNum)
		}
	})

	t.Run("deeply nested", func(t *testing.T) {
		r, err := NewParquetReader(fixture("deeply_nested.parquet"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		if r.rowNum != 20 {
			t.Errorf("expected 20 rows, got %d", r.rowNum)
		}
	})

	t.Run("multi rowgroup", func(t *testing.T) {
		r, err := NewParquetReader(fixture("multi_rowgroup.parquet"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		if r.rowNum != 90 {
			t.Errorf("expected 90 rows, got %d", r.rowNum)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		r, err := NewParquetReader(fixture("empty.parquet"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		if r.rowNum != 0 {
			t.Errorf("expected 0 rows, got %d", r.rowNum)
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		_, err := NewParquetReader("/tmp/does_not_exist.parquet")
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
	})

	t.Run("wrong magic header", func(t *testing.T) {
		tmp := filepath.Join(t.TempDir(), "bad.parquet")
		os.WriteFile(tmp, []byte("NOT1xxxxxxxxxxxx"), 0644)
		_, err := NewParquetReader(tmp)
		if err == nil {
			t.Fatal("expected error for bad magic header")
		}
	})

	t.Run("truncated file", func(t *testing.T) {
		tmp := filepath.Join(t.TempDir(), "trunc.parquet")
		os.WriteFile(tmp, []byte("PAR1xxxx"), 0644)
		_, err := NewParquetReader(tmp)
		if err == nil {
			t.Fatal("expected error for truncated file")
		}
	})

	t.Run("too small file", func(t *testing.T) {
		tmp := filepath.Join(t.TempDir(), "tiny.parquet")
		os.WriteFile(tmp, []byte("PAR1abcd"), 0644)
		_, err := NewParquetReader(tmp)
		if err == nil {
			t.Fatal("expected error for too-small file")
		}
	})
}

// --- Head ---

func TestHead(t *testing.T) {
	t.Run("flat/n=10", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Head(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		if rows[0]["id"] != "id_0" {
			t.Errorf("first row id: got %v, want id_0", rows[0]["id"])
		}
	})

	t.Run("flat/n=5", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Head(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("flat/n exceeds total", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Head(1000)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 100 {
			t.Errorf("expected 100 rows (clamped), got %d", len(rows))
		}
	})

	t.Run("nested struct", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("nested_struct.parquet"))
		defer r.Close()
		rows, err := r.Head(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		info, ok := rows[0]["info"]
		if !ok {
			t.Fatal("missing 'info' field in row")
		}
		infoMap, ok := info.(map[string]interface{})
		if !ok {
			t.Fatalf("info should be map, got %T", info)
		}
		if _, ok := infoMap["address"]; !ok {
			t.Error("missing nested 'address' field in info")
		}
	})

	t.Run("list of primitives", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("list_primitive.parquet"))
		defer r.Close()
		rows, err := r.Head(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tags, ok := rows[0]["tags"]
		if !ok {
			t.Fatal("missing 'tags' field")
		}
		tagList, ok := tags.([]interface{})
		if !ok {
			t.Fatalf("tags should be slice, got %T", tags)
		}
		if len(tagList) == 0 {
			t.Error("tags list should not be empty")
		}
	})

	t.Run("list of structs", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("list_struct.parquet"))
		defer r.Close()
		rows, err := r.Head(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		items, ok := rows[0]["items"]
		if !ok {
			t.Fatal("missing 'items' field")
		}
		itemList, ok := items.([]interface{})
		if !ok {
			t.Fatalf("items should be slice, got %T", items)
		}
		if len(itemList) == 0 {
			t.Fatal("items list should not be empty")
		}
		firstItem, ok := itemList[0].(map[string]interface{})
		if !ok {
			t.Fatalf("list item should be map, got %T", itemList[0])
		}
		if _, ok := firstItem["name"]; !ok {
			t.Error("missing 'name' in list struct item")
		}
	})

	t.Run("map simple", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("map_simple.parquet"))
		defer r.Close()
		rows, err := r.Head(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := rows[0]["props"]; !ok {
			t.Error("missing 'props' map field")
		}
		if _, ok := rows[0]["metrics"]; !ok {
			t.Error("missing 'metrics' map field")
		}
	})

	t.Run("map nested", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("map_nested.parquet"))
		defer r.Close()
		rows, err := r.Head(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := rows[0]["data"]; !ok {
			t.Error("missing 'data' map field")
		}
	})

	t.Run("deeply nested", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("deeply_nested.parquet"))
		defer r.Close()
		rows, err := r.Head(2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// Verify structure: data -> list -> struct -> meta -> struct -> scores (map)
		data, ok := rows[0]["data"]
		if !ok {
			t.Fatal("missing 'data' field")
		}
		dataList, ok := data.([]interface{})
		if !ok {
			t.Fatalf("data should be slice, got %T", data)
		}
		if len(dataList) == 0 {
			t.Fatal("data list should not be empty")
		}
		elem, ok := dataList[0].(map[string]interface{})
		if !ok {
			t.Fatalf("data element should be map, got %T", dataList[0])
		}
		meta, ok := elem["meta"]
		if !ok {
			t.Fatal("missing 'meta' in data element")
		}
		metaMap, ok := meta.(map[string]interface{})
		if !ok {
			t.Fatalf("meta should be map, got %T", meta)
		}
		if _, ok := metaMap["url"]; !ok {
			t.Error("missing 'url' in meta")
		}
		if _, ok := metaMap["scores"]; !ok {
			t.Error("missing 'scores' map in meta")
		}
	})

	t.Run("nullable", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("nullable.parquet"))
		defer r.Close()
		rows, err := r.Head(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 5 {
			t.Fatalf("expected 5 rows, got %d", len(rows))
		}
		// Row 1 (index 1) should have null name
		if rows[1]["name"] != nil {
			t.Errorf("expected null name in row 1, got %v", rows[1]["name"])
		}
	})

	t.Run("multi rowgroup", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("multi_rowgroup.parquet"))
		defer r.Close()
		rows, err := r.Head(50)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 50 {
			t.Errorf("expected 50 rows, got %d", len(rows))
		}
	})

	t.Run("empty file returns error", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("empty.parquet"))
		defer r.Close()
		_, err := r.Head(10)
		if err == nil {
			t.Error("expected error for empty file")
		}
	})
}

// --- Tail ---

func TestTail(t *testing.T) {
	t.Run("flat/last 10", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Tail(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		if rows[0]["id"] != "id_90" {
			t.Errorf("first tail row should be id_90, got %v", rows[0]["id"])
		}
		if rows[9]["id"] != "id_99" {
			t.Errorf("last tail row should be id_99, got %v", rows[9]["id"])
		}
	})

	t.Run("flat/last 5", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Tail(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
		if rows[0]["id"] != "id_95" {
			t.Errorf("first tail row should be id_95, got %v", rows[0]["id"])
		}
	})

	t.Run("flat/n exceeds total", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Tail(1000)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 100 {
			t.Errorf("expected 100 rows (clamped), got %d", len(rows))
		}
		if rows[0]["id"] != "id_0" {
			t.Errorf("first row should be id_0, got %v", rows[0]["id"])
		}
	})

	t.Run("nested struct", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("nested_struct.parquet"))
		defer r.Close()
		rows, err := r.Tail(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		if rows[2]["id"] != "id_49" {
			t.Errorf("last row should be id_49, got %v", rows[2]["id"])
		}
	})

	t.Run("deeply nested", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("deeply_nested.parquet"))
		defer r.Close()
		rows, err := r.Tail(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 5 {
			t.Fatalf("expected 5 rows, got %d", len(rows))
		}
		if rows[4]["id"] != "id_19" {
			t.Errorf("last row should be id_19, got %v", rows[4]["id"])
		}
	})

	t.Run("multi rowgroup", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("multi_rowgroup.parquet"))
		defer r.Close()
		rows, err := r.Tail(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		// Last row should be id_89 (90 rows total, 0-indexed)
		if rows[9]["id"] != "id_89" {
			t.Errorf("last row should be id_89, got %v", rows[9]["id"])
		}
	})

	t.Run("empty file returns error", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("empty.parquet"))
		defer r.Close()
		_, err := r.Tail(10)
		if err == nil {
			t.Error("expected error for empty file")
		}
	})
}

// --- Count ---

func TestCount(t *testing.T) {
	cases := []struct {
		file  string
		count int64
	}{
		{"flat.parquet", 100},
		{"nested_struct.parquet", 50},
		{"list_primitive.parquet", 50},
		{"list_struct.parquet", 50},
		{"map_simple.parquet", 50},
		{"map_nested.parquet", 50},
		{"deeply_nested.parquet", 20},
		{"nullable.parquet", 5},
		{"multi_rowgroup.parquet", 90},
		{"empty.parquet", 0},
		{"large.parquet", 10000},
	}

	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			r, err := NewParquetReader(fixture(tc.file))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer r.Close()
			count, err := r.Count()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tc.count {
				t.Errorf("expected %d rows, got %d", tc.count, count)
			}
		})
	}
}

// --- Schema ---

func TestGetSchema(t *testing.T) {
	t.Run("flat schema", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		s, err := r.GetSchema()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s == "" {
			t.Fatal("schema should not be empty")
		}
		for _, field := range []string{"id", "name", "age", "score", "active"} {
			if !bytes.Contains([]byte(s), []byte(field)) {
				t.Errorf("schema should contain field %q", field)
			}
		}
	})

	t.Run("nested has group", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("nested_struct.parquet"))
		defer r.Close()
		s, err := r.GetSchema()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Contains([]byte(s), []byte("group")) {
			t.Error("nested schema should contain 'group' keyword")
		}
	})

	t.Run("map has MAP", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("map_simple.parquet"))
		defer r.Close()
		s, err := r.GetSchema()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Contains([]byte(s), []byte("MAP")) {
			t.Error("map schema should contain 'MAP' keyword")
		}
	})

	t.Run("deeply nested schema", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("deeply_nested.parquet"))
		defer r.Close()
		s, err := r.GetSchema()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, field := range []string{"data", "meta", "url", "images", "trace_id"} {
			if !bytes.Contains([]byte(s), []byte(field)) {
				t.Errorf("deeply nested schema should contain field %q", field)
			}
		}
	})

	t.Run("empty file", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("empty.parquet"))
		defer r.Close()
		s, err := r.GetSchema()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Contains([]byte(s), []byte("0 rows")) {
			t.Error("empty file schema should report 0 rows")
		}
	})
}

// --- PrintJSON ---

func TestPrintJSON(t *testing.T) {
	t.Run("compact output", func(t *testing.T) {
		data := []map[string]interface{}{
			{"id": "a", "value": 1},
			{"id": "b", "value": 2},
		}
		var buf bytes.Buffer
		err := PrintJSON(data, &buf, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
		if len(lines) != 2 {
			t.Errorf("expected 2 lines, got %d", len(lines))
		}
		var row map[string]interface{}
		json.Unmarshal(lines[0], &row)
		if row["id"] != "a" {
			t.Errorf("first row id should be 'a', got %v", row["id"])
		}
	})

	t.Run("pretty output", func(t *testing.T) {
		data := []map[string]interface{}{
			{"id": "a"},
		}
		var buf bytes.Buffer
		err := PrintJSON(data, &buf, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Contains(buf.Bytes(), []byte("  ")) {
			t.Error("pretty output should contain indentation")
		}
	})

	t.Run("nested data", func(t *testing.T) {
		data := []map[string]interface{}{
			{"id": "a", "nested": map[string]interface{}{"key": "val"}},
		}
		var buf bytes.Buffer
		PrintJSON(data, &buf, false)
		var row map[string]interface{}
		json.Unmarshal(buf.Bytes(), &row)
		nested, ok := row["nested"].(map[string]interface{})
		if !ok {
			t.Fatal("nested field should be object")
		}
		if nested["key"] != "val" {
			t.Errorf("nested.key should be 'val', got %v", nested["key"])
		}
	})

	t.Run("empty data", func(t *testing.T) {
		var buf bytes.Buffer
		err := PrintJSON(nil, &buf, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if buf.Len() != 0 {
			t.Error("empty data should produce no output")
		}
	})
}

// --- JSON round-trip for complex types ---

func TestHeadJSONRoundTrip(t *testing.T) {
	cases := []struct {
		file string
		n    int
	}{
		{"flat.parquet", 5},
		{"nested_struct.parquet", 5},
		{"list_primitive.parquet", 5},
		{"list_struct.parquet", 5},
		{"map_simple.parquet", 5},
		{"deeply_nested.parquet", 5},
		{"nullable.parquet", 5},
	}

	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			r, err := NewParquetReader(fixture(tc.file))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer r.Close()

			rows, err := r.Head(tc.n)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var buf bytes.Buffer
			if err := PrintJSON(rows, &buf, false); err != nil {
				t.Fatalf("PrintJSON failed: %v", err)
			}

			// Verify each line is valid JSON
			lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
			if len(lines) != tc.n {
				t.Errorf("expected %d JSON lines, got %d", tc.n, len(lines))
			}
			for i, line := range lines {
				var obj map[string]interface{}
				if err := json.Unmarshal(line, &obj); err != nil {
					t.Errorf("line %d is not valid JSON: %v\n%s", i, err, string(line))
				}
			}
		})
	}
}

// --- Sample ---

func TestSample(t *testing.T) {
	t.Run("flat/n=5", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Sample(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("n exceeds total returns all", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("nullable.parquet"))
		defer r.Close()
		rows, err := r.Sample(100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 5 {
			t.Errorf("expected 5 rows (clamped), got %d", len(rows))
		}
	})

	t.Run("deeply nested", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("deeply_nested.parquet"))
		defer r.Close()
		rows, err := r.Sample(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		if _, ok := rows[0]["data"]; !ok {
			t.Error("missing 'data' field")
		}
	})

	t.Run("empty file returns error", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("empty.parquet"))
		defer r.Close()
		_, err := r.Sample(5)
		if err == nil {
			t.Error("expected error for empty file")
		}
	})

	t.Run("results are unique", func(t *testing.T) {
		r, _ := NewParquetReader(fixture("flat.parquet"))
		defer r.Close()
		rows, err := r.Sample(20)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ids := make(map[interface{}]bool)
		for _, row := range rows {
			id := row["id"]
			if ids[id] {
				t.Errorf("duplicate id: %v", id)
			}
			ids[id] = true
		}
	})
}

// --- Benchmarks ---

func BenchmarkHead(b *testing.B) {
	benchmarks := []struct {
		name string
		file string
		n    int
	}{
		{"flat_100/head10", "flat.parquet", 10},
		{"flat_100/head100", "flat.parquet", 100},
		{"large_10k/head10", "large.parquet", 10},
		{"large_10k/head100", "large.parquet", 100},
		{"large_10k/head1000", "large.parquet", 1000},
		{"nested/head10", "nested_struct.parquet", 10},
		{"map/head10", "map_simple.parquet", 10},
		{"deeply_nested/head10", "deeply_nested.parquet", 10},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r, err := NewParquetReader(fixture(bm.file))
				if err != nil {
					b.Fatalf("unexpected error: %v", err)
				}
				_, err = r.Head(bm.n)
				if err != nil {
					b.Fatalf("unexpected error: %v", err)
				}
				r.Close()
			}
		})
	}
}

func BenchmarkCount(b *testing.B) {
	b.Run("large_10k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r, _ := NewParquetReader(fixture("large.parquet"))
			r.Count()
			r.Close()
		}
	})
}
