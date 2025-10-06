package clickhouseconn

import (
	"sync/atomic"
)

// Metrics содержит счетчики успешных и ошибочных вставок
type Metrics struct {
	SuccessInserts uint64 // число успешных батчей
	FailedInserts  uint64 // число неуспешных батчей
	LastBatchSize  uint64 // размер последнего батча (успешного или нет)
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
