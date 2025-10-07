package clickhousecache

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
)

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
	fields := a.fieldnames
	table := a.tablename

	for try = 0; try <= a.maxRetries; try++ {
		ctx, cancel := context.WithTimeout(context.Background(), a.config.WriteTimeout)
		// defer cancel() — УБРАТЬ defer из цикла!

		// 1. Создаём блок вставки
		block, err := a.clickhouseDB.PrepareBatch(ctx, "INSERT INTO "+table+" ("+strings.Join(fields, ",")+")")
		if err != nil {
			log.Printf("[ClickhouseStorage] PrepareBatch error: %v", err)
			lastErr = err
			cancel()
			goto retry
		}

		// 2. Добавляем строки в batch
		for _, row := range batch {
			args := buildArgs(fields, row)
			if err := block.Append(args...); err != nil {
				log.Printf("[ClickhouseStorage] block.Append error: %v", err)
				lastErr = err
				break
			}
		}

		if lastErr != nil {
			cancel()
			goto retry
		}

		// 3. Отправляем batch
		if err := block.Send(); err != nil {
			log.Printf("[ClickhouseStorage] block.Send error: %v", err)
			lastErr = err
			cancel()
			goto retry
		}

		cancel()
		log.Printf("[ClickhouseStorage] store: SUCCESS, wrote %d rows", len(batch))
		a.Metrics.IncSuccess(len(batch))
		return // SUCCESS

	retry:
		if isConnErr(lastErr) {
			log.Printf("[ClickhouseStorage] store: detected connection error, trying reconnect")
			if recErr := a.reconnectDB(); recErr != nil {
				log.Printf("[ClickhouseStorage] store: reconnectDB failed: %v", recErr)
			}
		}
		a.Metrics.IncFailed(len(batch))
		if a.OnBatchError != nil {
			a.OnBatchError(batch, lastErr)
		}
		if try < a.maxRetries {
			backoffDur = a.backoffBase * (1 << try)
			if backoffDur > a.backoffMax {
				backoffDur = a.backoffMax
			}
			log.Printf("[ClickhouseStorage] store: sleeping for %v before retry", backoffDur)
			time.Sleep(backoffDur)
			continue
		}
		log.Printf("[ClickhouseStorage] store: all retries failed, returning data to buffer")
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
