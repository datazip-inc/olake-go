package driver

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/kafka/internal/kafkatest"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

func TestParseKafkaData(t *testing.T) {
	k := Kafka{} // schemaRegistryClient is nil
	tests := []struct {
		name          string
		record        *kgo.Record
		wantValue     map[string]interface{}
		wantKey       string
		wantKeyFields map[string]interface{}
		wantErr       bool
	}{
		{
			name: "json value and object key",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   []byte(`{"id":"k1"}`),
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
		},
		{
			name: "tombstone nil value keeps key",
			record: &kgo.Record{
				Value: nil,
				Key:   []byte(`{"id":"k1"}`),
			},
			wantValue:     nil,
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
		},
		{
			name: "nil key",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   nil,
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey: "",
		},
		{
			name: "empty-bytes key",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   []byte{},
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey: "",
		},
		{
			name: "unparseable key falls back to canonicalize",
			record: &kgo.Record{
				Value: []byte(`{"a":1,"b":2}`),
				Key:   []byte(`not-json`),
			},
			wantValue: map[string]interface{}{
				"a": json.Number("1"),
				"b": json.Number("2"),
			},
			wantKey: k.canonicalizeKafkaKey([]byte(`not-json`)),
		},
		{
			name: "invalid json value errors",
			record: &kgo.Record{
				Value: []byte("not-json"),
			},
			wantErr: true,
		},
		{
			name: "json object key with spaces is canonicalized",
			record: &kgo.Record{
				Value: []byte(`{"a":1}`),
				Key:   []byte(`{"id":   "k1"}`),
			},
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
			wantValue:     map[string]interface{}{"a": json.Number("1")},
		},
		{
			name: "json object key field order is canonicalized",
			record: &kgo.Record{
				Value: []byte(`{"a":1}`),
				Key:   []byte(`{"b":1,"a":2}`),
			},
			wantKey: `{"a":2,"b":1}`,
			wantKeyFields: map[string]interface{}{
				"a": json.Number("2"),
				"b": json.Number("1"),
			},
			wantValue: map[string]interface{}{"a": json.Number("1")},
		},
		{
			name: "tombstone spaced object key matches create",
			record: &kgo.Record{
				Value: nil,
				Key:   []byte(`{"id": "k1"}`),
			},
			wantValue:     nil,
			wantKey:       `{"id":"k1"}`,
			wantKeyFields: map[string]interface{}{"id": "k1"},
		},
		{
			name: "8-byte binary key is not treated as confluent payload",
			record: &kgo.Record{
				Value: []byte(`{"v":1}`),
				Key:   []byte{0, 0, 0, 0, 0, 0, 0, 5},
			},
			wantValue: map[string]interface{}{"v": json.Number("1")},
			wantKey:   "AAAAAAAAAAU=",
		},
		{
			name: "tombstone 8-byte binary key matches live",
			record: &kgo.Record{
				Value: nil,
				Key:   []byte{0, 0, 0, 0, 0, 0, 0, 5},
			},
			wantValue: nil,
			wantKey:   "AAAAAAAAAAU=",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, key, keyFields, err := k.parseKafkaData(tt.record)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, value)
			assert.Equal(t, tt.wantKey, key)
			assert.Equal(t, tt.wantKeyFields, keyFields)
		})
	}
}

func TestKafkaStreamChanges(t *testing.T) {
	testutils.UseStateFile(t)
	alice, bob := []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":2,"name":"bob"}`)
	testCases := []struct {
		name string
		// messages go to the test's topic in this order; a nil value is a tombstone.
		messages [][]byte
		want     []abstract.CDCChange
	}{
		{
			name:     "streams a message",
			messages: [][]byte{alice},
			want: []abstract.CDCChange{
				{Kind: "create", Data: map[string]any{"id": json.Number("1"), "name": "alice", Partition: int32(0), Offset: int64(0)}},
			},
		},
		{
			name:     "streams messages in offset order",
			messages: [][]byte{alice, bob},
			want: []abstract.CDCChange{
				{Kind: "create", Data: map[string]any{"id": json.Number("1"), "name": "alice", Partition: int32(0), Offset: int64(0)}},
				{Kind: "create", Data: map[string]any{"id": json.Number("2"), "name": "bob", Partition: int32(0), Offset: int64(1)}},
			},
		},
		{
			name:     "skips a tombstone",
			messages: [][]byte{nil, alice},
			want: []abstract.CDCChange{
				{Kind: "create", Data: map[string]any{"id": json.Number("1"), "name": "alice", Partition: int32(0), Offset: int64(1)}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Every case reads its own topic with its own consumer group, so a message leaking between them shows in got.
			t.Parallel()
			h := testhandler.New(t)
			k := &Kafka{config: testConfig(h)}
			require.NoError(t, k.Setup(t.Context()))
			t.Cleanup(func() { _ = k.Close() })
			topic := h.Database(kafkatest.Source)
			for _, message := range tc.messages {
				require.NoError(t, k.client.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: message}).FirstErr())
			}
			k.SetupState(&types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType})
			require.NoError(t, k.PreCDC(t.Context(), []types.StreamInterface{types.NewStream(topic, "topics", nil).Wrap(0)}))

			var got []abstract.CDCChange
			_, err := k.StreamChanges(t.Context(), 0, nil, func(_ context.Context, change abstract.CDCChange) error {
				// the broker stamps the time it appended the message
				delete(change.Data, KafkaTimestamp)
				got = append(got, abstract.CDCChange{Kind: change.Kind, Data: change.Data})
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, k.PostCDC(t.Context(), 0))
			require.Equal(t, tc.want, got)
		})
	}
}
