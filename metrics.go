package clickhouseconn

import (
	"sync/atomic"
)

// Metrics holds counters for monitoring ClickHouse storage operations.
type Metrics struct {
	SuccessInserts uint64 // count of successful batches
	FailedInserts  uint64 // count of failed batches
	LastBatchSize  uint64 // size of the last batch (successful or not)
}

func (m *Metrics) IncSuccess(batchSize int) {
	atomic.AddUint64(&m.SuccessInserts, 1)
	atomic.StoreUint64(&m.LastBatchSize, uint64(batchSize))
}

func (m *Metrics) IncFailed(batchSize int) {
	atomic.AddUint64(&m.FailedInserts, 1)
	atomic.StoreUint64(&m.LastBatchSize, uint64(batchSize))
}

func (m *Metrics) Get() Metrics {
	return Metrics{
		SuccessInserts: atomic.LoadUint64(&m.SuccessInserts),
		FailedInserts:  atomic.LoadUint64(&m.FailedInserts),
		LastBatchSize:  atomic.LoadUint64(&m.LastBatchSize),
	}
}
