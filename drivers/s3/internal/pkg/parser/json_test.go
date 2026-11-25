package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONParser_InferSchema_NumbersAsFloat64(t *testing.T) {
	jsonData := `{"id": 1, "name": "Alice", "price": 99.99, "quantity": 10, "active": true}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Check that integer numbers are inferred as Float64 (JSON numbers are float64)
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id (integer) should be inferred as Float64")

	// Check that float numbers are inferred as Float64
	priceType, err := result.Schema.GetType("price")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, priceType, "price (float) should be inferred as Float64")

	// Check that quantity (integer) is also Float64
	quantityType, err := result.Schema.GetType("quantity")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, quantityType, "quantity (integer) should be inferred as Float64")

	// Check boolean
	activeType, err := result.Schema.GetType("active")
	require.NoError(t, err)
	assert.Equal(t, types.Bool, activeType, "active should be inferred as Bool")

	// Check string
	nameType, err := result.Schema.GetType("name")
	require.NoError(t, err)
	assert.Equal(t, types.String, nameType, "name should be inferred as String")
}

func TestJSONParser_InferSchema_JSONLFormat(t *testing.T) {
	jsonlData := `{"id": 1, "value": 100.5}
{"id": 2, "value": 200.75}
{"id": 3, "value": 300.25}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Both id and value should be Float64
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id should be Float64")

	valueType, err := result.Schema.GetType("value")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, valueType, "value should be Float64")
}

func TestJSONParser_InferSchema_JSONArrayFormat(t *testing.T) {
	jsonArrayData := `[
	{"id": 1, "score": 95.5},
	{"id": 2, "score": 87.25},
	{"id": 3, "score": 92.0}
]`

	config := JSONConfig{
		LineDelimited: false,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonArrayData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Both id and score should be Float64
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id should be Float64")

	scoreType, err := result.Schema.GetType("score")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, scoreType, "score should be Float64")
}

func TestJSONParser_InferSchema_ComplexTypes(t *testing.T) {
	jsonData := `{"name": "test", "metadata": {"key": "value"}, "tags": ["a", "b", "c"]}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Check string
	nameType, err := result.Schema.GetType("name")
	require.NoError(t, err)
	assert.Equal(t, types.String, nameType, "name should be String")

	// Check object - typeutils returns Object type for maps
	metadataType, err := result.Schema.GetType("metadata")
	require.NoError(t, err)
	assert.Equal(t, types.Object, metadataType, "metadata should be Object")

	// Check array - typeutils returns Array type for slices
	tagsType, err := result.Schema.GetType("tags")
	require.NoError(t, err)
	assert.Equal(t, types.Array, tagsType, "tags should be Array")
}

func TestJSONParser_InferSchema_MixedTypes(t *testing.T) {
	// When a field has different types across records, typeutils resolves to common ancestor
	jsonlData := `{"field": 123}
{"field": "text"}
{"field": true}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Mixed types should resolve to String (common ancestor)
	fieldType, err := result.Schema.GetType("field")
	require.NoError(t, err)
	assert.Equal(t, types.String, fieldType, "mixed types should resolve to String")
}

func TestJSONParser_StreamRecords_TypeConsistency(t *testing.T) {
	jsonlData := `{"id": 1, "count": 10, "price": 99.99}
{"id": 2, "count": 20, "price": 149.50}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	stream.UpsertField("id", types.Float64, true, false)
	stream.UpsertField("count", types.Float64, true, false)
	stream.UpsertField("price", types.Float64, true, false)

	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonlData)

	records := []map[string]any{}
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}

	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 2)

	// Check that all numeric values are float64
	for i, record := range records {
		assert.IsType(t, float64(0), record["id"], "id in record %d should be float64", i)
		assert.IsType(t, float64(0), record["count"], "count in record %d should be float64", i)
		assert.IsType(t, float64(0), record["price"], "price in record %d should be float64", i)
	}
}

func TestJSONParser_StreamRecords_MalformedJSONLReturnsRecordError(t *testing.T) {
	jsonlData := `{"id": 1, "name": "Alice"}
{"id": invalid, "name": "Broken"}
{"id": 3, "name": "Charlie"}`

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelAfterDoneChecks{
		Context:           baseCtx,
		cancel:            cancel,
		checksUntilCancel: 3,
	}

	parser := NewJSONParser(JSONConfig{LineDelimited: true}, types.NewStream("test", "test", nil))
	var records []map[string]any
	err := parser.StreamRecords(ctx, strings.NewReader(jsonlData), func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	})

	require.ErrorContains(t, err, "JSON record 2")
	require.Len(t, records, 1)
	assert.Equal(t, float64(1), records[0]["id"])
}

func TestJSONParser_InferSchema_NullableFields(t *testing.T) {
	// Fields that don't appear in all records should be nullable
	jsonlData := `{"id": 1, "name": "Alice"}
{"id": 2}
{"id": 3, "name": "Charlie"}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// id should be present
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id should be Float64")

	// name should be present
	nameType, err := result.Schema.GetType("name")
	require.NoError(t, err)
	assert.Equal(t, types.String, nameType, "name should be String")
}

type cancelAfterDoneChecks struct {
	context.Context
	cancel            context.CancelFunc
	checksUntilCancel int
}

func (c *cancelAfterDoneChecks) Done() <-chan struct{} {
	c.checksUntilCancel--
	if c.checksUntilCancel == 0 {
		c.cancel()
	}
	return c.Context.Done()
}
