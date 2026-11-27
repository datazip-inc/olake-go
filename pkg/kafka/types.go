package kafka

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReaderManager exit modes used to control CDC processing flow.
const (
	normalProcessing int32 = iota // normal processing state
	gracefulExit                  // stop processing on rebalance (without triggering retries)
	nonRetryableExit              // stop processing due to unrecoverable kafka errors
)

// ReaderConfig holds configuration for creating Kafka readers
type ReaderConfig struct {
	MaxThreads                  int
	ThreadsEqualTotalPartitions bool
	ConsumerGroupID             string
	Dialer                      []kgo.Opt
	AdminClient                 *kadm.Client
	Membership                  GroupMembership
}

// GroupMembership controls how readers join the consumer group. The zero value keeps the default behavior.
type GroupMembership struct {
	InstanceID       func(readerIndex int) string // static instance id per reader; nil uses group_<ULID>
	SessionTimeout   time.Duration                // 0 uses the franz-go default (45s)
	KeepMembers      bool                         // skip removing existing group members before joining
	ReleaseAfterJoin bool                         // close readers after assignment; static seats stay until the session timeout
	// ExpectedAssignment, when set, fails CreateReaders if a reader's assigned partitions differ from the returned ones
	ExpectedAssignment func(readerIndex int) []types.PartitionKey
}

type kafkaReader struct {
	id       string
	clientID string
	reader   *kgo.Client
}

// ReaderManager manages Kafka readers and their metadata
type ReaderManager struct {
	config        ReaderConfig
	readers       []*kafkaReader
	topics        []string                           // topics to be consumed
	partitionMeta map[string]types.PartitionMetaData // get per-partition boundaries
	exitMode      atomic.Int32                       // normalProcessing | gracefulExit | nonRetryableExit
	generationID  atomic.Int32                       // consumer group generationId: used to detect rebalances
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	partitionMeta map[string]types.PartitionMetaData
}

// NewCustomGroupBalancer returns a balancer that assigns only partitions present in partitionMeta.
func NewCustomGroupBalancer(partitionMeta map[string]types.PartitionMetaData) *CustomGroupBalancer {
	return &CustomGroupBalancer{partitionMeta: partitionMeta}
}

// SchemaRegistryClient holds the schema registry client information
type SchemaRegistryClient struct {
	Endpoint string `json:"endpoint"`

	// Authentication
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	BearerToken string `json:"bearer_token,omitempty"`

	httpClient *http.Client
	schemaMap  sync.Map // map[uint32]*RegisteredSchema (key -> schemaID)
}
