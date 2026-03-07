package clickgraph

import (
	"testing"

	ffi "github.com/genezhang/clickgraph-go/clickgraph_ffi"
)

// --- Unit tests for pure Go logic (no chdb required) ---

func TestToGoValue_Null(t *testing.T) {
	result := toGoValue(ffi.ValueNull{})
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestToGoValue_Bool(t *testing.T) {
	result := toGoValue(ffi.ValueBool{V: true})
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestToGoValue_Int64(t *testing.T) {
	result := toGoValue(ffi.ValueInt64{V: 42})
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestToGoValue_Float64(t *testing.T) {
	result := toGoValue(ffi.ValueFloat64{V: 3.14})
	if result != 3.14 {
		t.Errorf("expected 3.14, got %v", result)
	}
}

func TestToGoValue_String(t *testing.T) {
	result := toGoValue(ffi.ValueString{V: "hello"})
	if result != "hello" {
		t.Errorf("expected hello, got %v", result)
	}
}

func TestToGoValue_List(t *testing.T) {
	result := toGoValue(ffi.ValueList{
		Items: []ffi.Value{
			ffi.ValueInt64{V: 1},
			ffi.ValueInt64{V: 2},
			ffi.ValueString{V: "three"},
		},
	})
	list, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 items, got %d", len(list))
	}
	if list[0] != int64(1) || list[1] != int64(2) || list[2] != "three" {
		t.Errorf("unexpected list values: %v", list)
	}
}

func TestToGoValue_Map(t *testing.T) {
	result := toGoValue(ffi.ValueMap{
		Entries: []ffi.MapEntry{
			{Key: "name", Value: ffi.ValueString{V: "Alice"}},
			{Key: "age", Value: ffi.ValueInt64{V: 30}},
		},
	})
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map[string]interface{}, got %T", result)
	}
	if m["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", m["name"])
	}
	if m["age"] != int64(30) {
		t.Errorf("expected 30, got %v", m["age"])
	}
}

func TestRowGet(t *testing.T) {
	row := Row{
		columns: []string{"name", "age"},
		values: []ffi.Value{
			ffi.ValueString{V: "Bob"},
			ffi.ValueInt64{V: 25},
		},
	}

	if row.Get("name") != "Bob" {
		t.Errorf("expected Bob, got %v", row.Get("name"))
	}
	if row.Get("age") != int64(25) {
		t.Errorf("expected 25, got %v", row.Get("age"))
	}
	if row.Get("missing") != nil {
		t.Errorf("expected nil for missing column, got %v", row.Get("missing"))
	}
}

func TestRowAsMap(t *testing.T) {
	row := Row{
		columns: []string{"x", "y"},
		values: []ffi.Value{
			ffi.ValueFloat64{V: 1.5},
			ffi.ValueNull{},
		},
	}
	m := row.AsMap()
	if m["x"] != 1.5 {
		t.Errorf("expected 1.5, got %v", m["x"])
	}
	if m["y"] != nil {
		t.Errorf("expected nil, got %v", m["y"])
	}
}

func TestRowValues(t *testing.T) {
	row := Row{
		columns: []string{"a", "b"},
		values: []ffi.Value{
			ffi.ValueBool{V: false},
			ffi.ValueString{V: "yes"},
		},
	}
	vals := row.Values()
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
	if vals[0] != false {
		t.Errorf("expected false, got %v", vals[0])
	}
	if vals[1] != "yes" {
		t.Errorf("expected yes, got %v", vals[1])
	}
}

func TestRowColumns(t *testing.T) {
	row := Row{
		columns: []string{"col1", "col2"},
		values:  []ffi.Value{ffi.ValueNull{}, ffi.ValueNull{}},
	}
	cols := row.Columns()
	if len(cols) != 2 || cols[0] != "col1" || cols[1] != "col2" {
		t.Errorf("unexpected columns: %v", cols)
	}
}

func TestStrPtr(t *testing.T) {
	if strPtr("") != nil {
		t.Error("expected nil for empty string")
	}
	p := strPtr("hello")
	if p == nil || *p != "hello" {
		t.Error("expected pointer to hello")
	}
}

func TestUint32Ptr(t *testing.T) {
	if uint32Ptr(0) != nil {
		t.Error("expected nil for zero")
	}
	p := uint32Ptr(8)
	if p == nil || *p != 8 {
		t.Error("expected pointer to 8")
	}
}

func TestOpenInvalidPath(t *testing.T) {
	_, err := Open("/nonexistent/schema.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent schema")
	}
}
