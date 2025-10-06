package clickhouseconn

import (
	"database/sql"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClickhouseStorage struct {
	quit         int64
	m            sync.Mutex
	clickhouseDB *sql.DB
	writeTime    time.Duration
	fieldnames   []string
	tablename    string
	data         []map[string]interface{}
	maxBatch     int
	jsonFields   map[string]struct{}
}

// InitClickhouseStorage
// tablename - table name to insert data into (ex: `racing`.`requests`)
// fieldnames - list of fields in the tablename
// writetime - how often write data to table
// maxbatch - if >0 then write data when data count >= maxbatch
func InitClickhouseStorage(db *sql.DB, tablename string, fieldnames []string, writeTime time.Duration, maxbatch int, jsonFields []string) (*ClickhouseStorage, error) {
	//check fieldnames in table
	err := db.QueryRow("SELECT " + strings.Join(fieldnames, ",") + " FROM " + tablename + " LIMIT 1").Err()
	if err != nil {
		return nil, err
	}
	var res ClickhouseStorage
	res.clickhouseDB = db
	res.fieldnames = fieldnames
	res.tablename = tablename
	res.writeTime = writeTime
	res.maxBatch = maxbatch
	res.jsonFields = make(map[string]struct{})
	for _, name := range jsonFields {
		res.jsonFields[name] = struct{}{}
	}
	//start scheduler to regular write data
	go res.work()
	return &res, nil
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
func (a *ClickhouseStorage) Exit() {
	atomic.StoreInt64(&a.quit, 1)
	a.store()
}
func (a *ClickhouseStorage) store() {
	var (
		tx   *sql.Tx
		stmt *sql.Stmt
		err  error
	)
	sqlstr := "INSERT INTO  " + a.tablename + " (" + strings.Join(a.fieldnames, ",") + ") VALUES (" + strings.Repeat("?", len(a.fieldnames)) + ")"

	if tx, err = a.clickhouseDB.Begin(); err != nil {
		log.Println(err.Error())
		return
	}

	if stmt, err = tx.Prepare(sqlstr); err != nil {
		log.Println(err.Error())
		return
	}
	a.m.Lock()
	temp := make([]map[string]interface{}, len(a.data))
	copy(temp, a.data)
	a.data = nil
	a.m.Unlock()

	for i := range temp {
		if _, err = stmt.Exec(buildArgs(a.fieldnames, temp[i])...); err != nil {
			log.Println(err.Error())
			a.m.Lock()
			a.data = append(a.data, temp...)
			a.m.Unlock()
			tx.Rollback()
			return
		}
	}

	if err = tx.Commit(); err != nil {
		log.Println(err.Error())
		a.m.Lock()
		a.data = append(a.data, temp...)
		a.m.Unlock()
		return
	}
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
