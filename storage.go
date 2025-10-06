package clickhouseconn

import (
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClickhouseStorage struct {
	quit         int64
	m            sync.Mutex
	clickhouseDB *sql.DB
	config       TClickHouseConfig

	writeTime  time.Duration
	fieldnames []string
	tablename  string
	data       []map[string]interface{}
	maxBatch   int
	jsonFields map[string]struct{}
	// retry policy
	maxRetries  int           // maximum number of retries
	backoffBase time.Duration // base delay between retries
	backoffMax  time.Duration // maximum delay between retries

	Metrics      Metrics
	OnBatchError func(batch []map[string]interface{}, err error)
}

// InitClickhouseStorage
// tablename - table name to insert data into (ex: `racing`.`requests`)
// fieldnames - list of fields in the tablename
// writetime - how often write data to table
// maxbatch - if >0 then write data when data count >= maxbatch
// jsonFields - список полей, которые нужно сериализовать в JSON-строку
func InitClickhouseStorageWithConfig(config TClickHouseConfig, tablename string, fieldnames []string, writeTime time.Duration, maxbatch int, jsonFields []string, maxRetries int, backoffBase, backoffMax time.Duration) (*ClickhouseStorage, error) {
	db, err := InitClickHouseDB(config)
	if err != nil {
		return nil, err
	}
	res := &ClickhouseStorage{
		clickhouseDB: db,
		config:       config,
		fieldnames:   fieldnames,
		tablename:    tablename,
		writeTime:    writeTime,
		maxBatch:     maxbatch,
		jsonFields:   make(map[string]struct{}),
		maxRetries:   maxRetries,
		backoffBase:  backoffBase,
		backoffMax:   backoffMax,
	}
	for _, name := range jsonFields {
		res.jsonFields[name] = struct{}{}
	}
	go res.work()
	return res, nil
}

// Add adds data to storage.
// You may use Struct2Map() func to convert struct to map
func (a *ClickhouseStorage) Add(s map[string]interface{}) {
	if atomic.LoadInt64(&a.quit) == 1 {
		return
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
					s[field] = ""
				}
			}
		}
	}
	a.m.Lock()
	a.data = append(a.data, s)
	trigger := a.maxBatch > 0 && len(a.data) >= a.maxBatch
	a.m.Unlock()
	if trigger {
		go a.store()
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
func (a *ClickhouseStorage) store() {
	a.m.Lock()
	temp := make([]map[string]interface{}, len(a.data))
	copy(temp, a.data)
	a.data = nil
	a.m.Unlock()

	if len(temp) == 0 {
		return
	}

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

		tx, err := a.clickhouseDB.Begin()
		if err != nil {
			lastErr = err
		} else {
			stmt, err := tx.Prepare(sqlstr)
			if err != nil {
				_ = tx.Rollback()
				lastErr = err
			} else {
				success := true
				for i := range batch {
					if _, err = stmt.Exec(buildArgs(a.fieldnames, batch[i])...); err != nil {
						success = false
						lastErr = err
						break
					}
				}
				if success {
					if err = tx.Commit(); err != nil {
						lastErr = err
					} else {
						a.Metrics.IncSuccess(len(batch))
						return // SUCCESS
					}
				} else {
					_ = tx.Rollback()
				}
			}
		}

		// --- Переконнект при потере соединения ---
		if isConnErr(lastErr) {
			if recErr := a.reconnectDB(); recErr == nil {
				// После реконнекта — одна быстрая попытка без увеличения try
				sqlstr := "INSERT INTO  " + a.tablename + " (" + strings.Join(a.fieldnames, ",") + ") VALUES (" + strings.Repeat("?", len(a.fieldnames)) + ")"
				tx2, err2 := a.clickhouseDB.Begin()
				if err2 == nil {
					stmt2, err2 := tx2.Prepare(sqlstr)
					if err2 == nil {
						success := true
						for i := range batch {
							if _, err2 = stmt2.Exec(buildArgs(a.fieldnames, batch[i])...); err2 != nil {
								success = false
								lastErr = err2
								break
							}
						}
						if success {
							if err2 = tx2.Commit(); err2 == nil {
								a.Metrics.IncSuccess(len(batch))
								return
							} else {
								lastErr = err2
							}
						} else {
							_ = tx2.Rollback()
						}
					} else {
						_ = tx2.Rollback()
						lastErr = err2
					}
				} else {
					lastErr = err2
				}
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
			time.Sleep(backoffDur)
			continue
		}
		// Если все попытки исчерпаны — вернуть данные в буфер
		a.m.Lock()
		a.data = append(a.data, batch...)
		a.m.Unlock()
		return
	}
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
	db, err := InitClickHouseDB(a.config)
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
