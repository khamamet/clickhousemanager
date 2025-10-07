package clickhousecache

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClickhouseStorage struct {
	Metrics      Metrics
	OnBatchError func(batch []map[string]any, err error)

	quit         int64
	m            sync.Mutex
	clickhouseDB *sql.DB
	config       TClickHouseConfig

	writeTime  time.Duration
	fieldnames []string
	tablename  string
	data       []map[string]any
	maxBatch   int
	jsonFields map[string]struct{}
	// retry policy
	maxRetries  int           // maximum number of retries
	backoffBase time.Duration // base delay between retries
	backoffMax  time.Duration // maximum delay between retries
}

// Add adds data to storage.
// You may use Struct2Map() func to convert struct to map
func (a *ClickhouseStorage) Add(s map[string]any) error {
	if atomic.LoadInt64(&a.quit) == 1 {
		log.Println("[ClickhouseStorage] Add called after quit, ignoring")
		return errors.New("storage is exiting")
	}
	// сериализуем JSON-поля
	for field := range a.jsonFields {
		if val, ok := s[field]; ok && val != nil {
			switch v := val.(type) {
			case string:
				// уже строка, ничего делать не надо
			default:
				b, err := json.Marshal(v)
				if err == nil {
					s[field] = string(b)
				} else {
					log.Printf("[ClickhouseStorage] Add: JSON marshal error for field=%s: %v\n", field, err)
					s[field] = ""
				}
			}
		}
	}
	log.Printf("[ClickhouseStorage] Add: adding to buffer: %+v\n", s)
	a.m.Lock()
	a.data = append(a.data, s)
	log.Printf("[ClickhouseStorage] Buffer size: %d (maxBatch=%d)\n", len(a.data), a.maxBatch)
	trigger := a.maxBatch > 0 && len(a.data) >= a.maxBatch
	a.m.Unlock()
	if trigger {
		log.Println("[ClickhouseStorage] Buffer reached maxBatch, triggering store()")
		go a.store()
	}
	return nil
}

func (a *ClickhouseStorage) store() {
	a.m.Lock()
	temp := make([]map[string]interface{}, len(a.data))
	copy(temp, a.data)
	a.data = nil
	a.m.Unlock()

	if len(temp) == 0 {
		log.Println("[ClickhouseStorage] store: no data to write")
		return
	}

	log.Printf("[ClickhouseStorage] store: writing batch of %d rows to ClickHouse\n", len(temp))

	var (
		try        int
		batch      = temp
		lastErr    error
		backoffDur time.Duration
	)
	// escape field and table names
	fieldsEscaped := make([]string, len(a.fieldnames))
	for i, f := range a.fieldnames {
		fieldsEscaped[i] = escapeIdent(f)
	}
	tableEscaped := escapeTableName(a.tablename)

	for try = 0; try <= a.maxRetries; try++ {
		sqlstr := "INSERT INTO " + tableEscaped + " (" + strings.Join(fieldsEscaped, ",") + ") VALUES (" + strings.Repeat("?", len(a.fieldnames)) + ")"
		log.Printf("[ClickhouseStorage] store: try=%d, SQL: %s\n", try, sqlstr)

		tx, err := a.clickhouseDB.Begin()
		if err != nil {
			log.Printf("[ClickhouseStorage] store: tx.Begin error: %v\n", err)
			lastErr = err
		} else {
			stmt, err := tx.Prepare(sqlstr)
			if err != nil {
				log.Printf("[ClickhouseStorage] store: Prepare error: %v\n", err)
				_ = tx.Rollback()
				lastErr = err
			} else {
				success := true
				for i := range batch {
					args := buildArgs(a.fieldnames, batch[i])
					log.Printf("[ClickhouseStorage] store: stmt.Exec args: %+v\n", args)
					if _, err = stmt.Exec(args...); err != nil {
						log.Printf("[ClickhouseStorage] store: stmt.Exec error: %v\n", err)
						success = false
						lastErr = err
						break
					}
				}
				if success {
					if err = tx.Commit(); err != nil {
						log.Printf("[ClickhouseStorage] store: Commit error: %v\n", err)
						lastErr = err
					} else {
						log.Printf("[ClickhouseStorage] store: SUCCESS, wrote %d rows\n", len(batch))
						a.Metrics.IncSuccess(len(batch))
						return // SUCCESS
					}
				} else {
					log.Printf("[ClickhouseStorage] store: rolling back due to error")
					_ = tx.Rollback()
				}
			}
		}

		// --- Переконнект при потере соединения ---
		if isConnErr(lastErr) {
			log.Printf("[ClickhouseStorage] store: detected connection error, trying reconnect")
			if recErr := a.reconnectDB(); recErr == nil {
				// После реконнекта — одна быстрая попытка без увеличения try
				sqlstr := "INSERT INTO  " + a.tablename + " (" + strings.Join(a.fieldnames, ",") + ") VALUES (" + strings.Repeat("?", len(a.fieldnames)) + ")"
				tx2, err2 := a.clickhouseDB.Begin()
				if err2 == nil {
					stmt2, err2 := tx2.Prepare(sqlstr)
					if err2 == nil {
						success := true
						for i := range batch {
							args := buildArgs(a.fieldnames, batch[i])
							log.Printf("[ClickhouseStorage] store: reconn stmt.Exec args: %+v\n", args)
							if _, err2 = stmt2.Exec(args...); err2 != nil {
								log.Printf("[ClickhouseStorage] store: reconn stmt.Exec error: %v\n", err2)
								success = false
								lastErr = err2
								break
							}
						}
						if success {
							if err2 = tx2.Commit(); err2 == nil {
								log.Printf("[ClickhouseStorage] store: reconn SUCCESS, wrote %d rows\n", len(batch))
								a.Metrics.IncSuccess(len(batch))
								return
							} else {
								log.Printf("[ClickhouseStorage] store: reconn Commit error: %v\n", err2)
								lastErr = err2
							}
						} else {
							log.Printf("[ClickhouseStorage] store: reconn rollback")
							_ = tx2.Rollback()
						}
					} else {
						log.Printf("[ClickhouseStorage] store: reconn Prepare error: %v\n", err2)
						_ = tx2.Rollback()
						lastErr = err2
					}
				} else {
					log.Printf("[ClickhouseStorage] store: reconn tx.Begin error: %v\n", err2)
					lastErr = err2
				}
			} else {
				log.Printf("[ClickhouseStorage] store: reconnectDB failed: %v\n", recErr)
			}
		}
		a.Metrics.IncFailed(len(batch))
		if a.OnBatchError != nil {
			a.OnBatchError(batch, lastErr)
		}
		if try < a.maxRetries {
			backoffDur = a.backoffBase * (1 << try) // экспоненциально: base, 2*base, 4*base...
			if backoffDur > a.backoffMax {
				backoffDur = a.backoffMax
			}
			log.Printf("[ClickhouseStorage] store: sleeping for %v before retry\n", backoffDur)
			time.Sleep(backoffDur)
			continue
		}
		// Если все попытки исчерпаны — вернуть данные в буфер
		log.Printf("[ClickhouseStorage] store: all retries failed, returning data to buffer\n")
		a.m.Lock()
		a.data = append(a.data, batch...)
		a.m.Unlock()
		return
	}
}

func (a *ClickhouseStorage) work() {
	ticker := time.NewTicker(a.writeTime)
	for {
		<-ticker.C
		a.store()
	}
}

// Exit - quit and store all data
func (a *ClickhouseStorage) Exit() {
	atomic.StoreInt64(&a.quit, 1)
	a.store()
}

// Store - store data to Clickhouse manually
func (a *ClickhouseStorage) Store() {
	a.store()
}
func escapeIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
func escapeTableName(tbl string) string {
	parts := strings.Split(tbl, ".")
	for i, p := range parts {
		parts[i] = escapeIdent(p)
	}
	return strings.Join(parts, ".")
}

// isConnErr определяет, похоже ли это на ошибку потери соединения
func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "driver: bad connection") ||
		strings.Contains(msg, "connection reset")
}
func buildArgs(fn []string, m map[string]interface{}) (res []interface{}) {
	for i := range fn {
		if v, ok := m[fn[i]]; ok {
			res = append(res, v)
		} else {
			res = append(res, "")
		}
	}
	return res
}

func (a *ClickhouseStorage) reconnectDB() error {
	a.m.Lock()
	defer a.m.Unlock()
	db, err := initClickHouseDB(a.config)
	if err != nil {
		return err
	}
	old := a.clickhouseDB
	a.clickhouseDB = db
	if old != nil {
		_ = old.Close()
	}
	return nil
}
