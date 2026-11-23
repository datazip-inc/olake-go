package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValueBytes(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want int64
	}{
		{"nil", nil, 0},
		{"string", "hello", 5},
		{"empty string", "", 0},
		{"bytes", []byte{1, 2, 3}, 3},
		{"bool", true, 1},
		{"int16", int16(42), 2},
		{"int32", int32(42), 4},
		{"float32", float32(3.14), 4},
		{"int", 42, 8},
		{"int64", int64(42), 8},
		{"float64", 3.14, 8},
		{"time", time.Now(), 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, valueBytes(tc.val))
		})
	}
}

func TestValueBytesNestedMap(t *testing.T) {
	m := map[string]any{
		"a": "hello",   // 5
		"b": int64(42), // 8
	}
	assert.Equal(t, int64(13), valueBytes(m))
}

func TestValueBytesNestedSlice(t *testing.T) {
	s := []any{"ab", int64(1)} // 2 + 8
	assert.Equal(t, int64(10), valueBytes(s))
}

func TestValueBytesDeeplyNested(t *testing.T) {
	inner := map[string]any{"x": "val"} // 3
	outer := map[string]any{
		"nested": inner,
		"num":    int64(1), // 8
	}
	assert.Equal(t, int64(11), valueBytes(outer))
}

func TestRecordDataBytes(t *testing.T) {
	record := map[string]any{
		"name": "alice", // 5
		"age":  int64(30), // 8
		"tags": []any{"a", "bc"}, // 1 + 2
	}
	assert.Equal(t, int64(16), recordDataBytes(record))
}

func BenchmarkValueBytes(b *testing.B) {
	record := map[string]any{
		"id":   int64(1),
		"name": "benchmark-row-with-some-padding",
		"meta": map[string]any{
			"source": "s3",
			"nested": map[string]any{
				"deep": "value",
				"list": []any{"a", "b", "c"},
			},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valueBytes(record)
	}
}
