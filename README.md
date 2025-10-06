# clickhousemanager

A Go library for robust, asynchronous, batched data insertion into ClickHouse using `database/sql` and the [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) driver.  
The library features automatic JSON field serialization, time- and size-based batch flushing, error callbacks, metrics, and support for auto-reconnect and exponential backoff on write errors.

---

## Features

- **Asynchronous buffered inserts** with timer- and batch-size triggers
- **Automatic serialization for JSON fields**
- **Thread-safe**: safe to use from multiple goroutines
- **Error callback**: get the failed batch and error for custom handling
- **Success/failure metrics** available at runtime
- **Exponential backoff and retry** on batch write errors
- **Automatic reconnection** to ClickHouse on connection loss
- **Manual flush (`Store()`)** on demand
- **Utility to convert Go structs to map** with struct field `db` tags support
- **Safe escaping of table and field names** for SQL identifiers

---

## Quick Start

```go
import (
    "github.com/khamamet/clickhousemanager"
    "time"
)

// 1. Prepare ClickHouse connection config
cfg := clickhouseconn.TClickHouseConfig{
    ClkAddress:  []string{"127.0.0.1:9000"},
    ClkUserName: "default",
    ClkPassword: "password",
    ClkMaxConn:  10,
    ClkDBName:   "default",
}

// 2. Initialize storage for a table using config struct (defaults will be used for omitted fields)
storage, err := clickhouseconn.NewClickhouseStorage(clickhouseconn.ClickhouseStorageConfig{
    Config:     cfg,
    TableName:  "userservice.user_audit_history",
    FieldNames: []string{"user_id", "changed_at", "changes"},
    JSONFields: []string{"changes"},
    // Optional: WriteTime, MaxBatch, MaxRetries, BackoffBase, BackoffMax...
})
if err != nil {
    panic(err)
}

// 3. Add records (can be called from multiple goroutines)
type Audit struct {
    UserID    string                 `db:"user_id"`
    ChangedAt time.Time              `db:"changed_at"`
    Changes   map[string]interface{} `db:"changes"`
}
record := Audit{UserID: "xxx", ChangedAt: time.Now(), Changes: map[string]interface{}{"field": "value"}}
m := clickhouseconn.Struct2Map(record)
storage.Add(m)

// 4. Manual flush (optional)
storage.Store()

// 5. Graceful shutdown: flush remaining data
storage.Exit()
```

---

## API Overview

- **InitClickHouseDB(TClickHouseConfig) (\*sql.DB, error)**  
  Connect to ClickHouse using config struct.

- **NewClickhouseStorage(ClickhouseStorageConfig) (\*ClickhouseStorage, error)**  
  Create a storage buffer for asynchronous, batched inserts with optional parameters.

- **Add(map[string]interface{})**  
  Add a record to the buffer.

- **Store()**  
  Immediately flush the buffer to ClickHouse.

- **Exit()**  
  Flush all remaining data and terminate the background writer.

- **Struct2Map(struct)**  
  Convert a struct to map for insertion (uses `db` struct tags).

- **OnBatchError**  
  Set a callback: `func(batch []map[string]interface{}, err error)` — called on each failed batch write.

- **Metrics**  
  Get real-time stats: number of successful/failed batch writes, last batch size.

---

## Advanced Features

- **Automatic JSON serialization:**  
  Fields marked in `jsonFields` are always serialized to JSON string before insertion.

- **Exponential backoff and retry:**  
  On write error, the batch is retried with increasing delay, up to `maxRetries` and `backoffMax`.

- **Automatic reconnect:**  
  If a write fails due to connection loss, the library will transparently reconnect and retry.

- **Manual and automatic flush:**  
  Data is flushed either when the timer triggers, when enough records are buffered, or via explicit `Store()`.

- **Identifier escaping:**  
  All table and field names are safely escaped with backticks to avoid SQL injection and reserved keywords issues.

- **Thread safety:**  
  It is safe to call `Add` and `Store` from any number of goroutines.

---

## Example: Error callback and metrics

```go
storage.OnBatchError = func(batch []map[string]interface{}, err error) {
    log.Printf("Failed to write batch of %d: %v", len(batch), err)
}

metrics := storage.Metrics.Get()
fmt.Printf("Success: %d, Failed: %d, LastBatch: %d\n",
    metrics.SuccessInserts, metrics.FailedInserts, metrics.LastBatchSize)
```

---

## Recommendations

- Escaping of table and field names is done automatically, but for best safety, avoid user-controlled names.
- For high-throughput, consider tuning batch size and flush interval.
- Use `Struct2Map` with appropriate `db` tags for best compatibility.

---

## License

MIT License
