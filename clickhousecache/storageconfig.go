package clickhousecache

import (
	"errors"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickhouseStorageConfig struct {
	Config      TClickHouseConfig // параметры подключения к ClickHouse
	TableName   string            // имя таблицы в ClickHouse
	FieldNames  []string          // список полей (колонок) в таблице
	WriteTime   time.Duration     // период записи данных из буфера в ClickHouse
	MaxBatch    int               // максимальный размер батча для записи
	JSONFields  []string          // список полей, которые нужно сериализовать в JSON
	MaxRetries  int               // максимальное количество попыток записи при ошибке
	BackoffBase time.Duration     // базовая задержка для экспоненциального бэкоффа
	BackoffMax  time.Duration     // максимальная задержка для бэкоффа
}

type ClickhouseStorage struct {
	Metrics      Metrics
	OnBatchError func(batch []map[string]any, err error)

	quit         int64
	m            sync.Mutex
	clickhouseDB clickhouse.Conn // теперь это clickhouse.Conn, а не *sql.DB
	config       TClickHouseConfig

	writeTime  time.Duration
	fieldnames []string
	tablename  string
	data       []map[string]any
	maxBatch   int
	jsonFields map[string]struct{}
	// retry policy
	maxRetries  int
	backoffBase time.Duration
	backoffMax  time.Duration
}

var (
	ErrNoTableName          = errors.New("no table name provided in config")
	ErrNoFieldNames         = errors.New("no field names provided in config")
	ErrNoClickhouseAddress  = errors.New("no ClickHouse address provided in config")
	ErrNoClickhouseDBName   = errors.New("no ClickHouse database name provided in config")
	ErrNoClickhouseUserName = errors.New("no ClickHouse user name provided in config")
)

func NewClickhouseStorage(cfg ClickhouseStorageConfig) (*ClickhouseStorage, error) {
	//check mandatory params
	if cfg.TableName == "" {
		return nil, ErrNoTableName
	}
	if len(cfg.FieldNames) == 0 {
		return nil, ErrNoFieldNames
	}
	if len(cfg.Config.ClkAddress) == 0 {
		return nil, ErrNoClickhouseAddress
	}
	if cfg.Config.ClkDBName == "" {
		return nil, ErrNoClickhouseDBName
	}
	if cfg.Config.ClkUserName == "" {
		return nil, ErrNoClickhouseUserName
	}
	if cfg.Config.MaxOpenConns <= 0 {
		cfg.Config.MaxOpenConns = 10
	}

	// Defaults
	if cfg.WriteTime == 0 {
		cfg.WriteTime = 2 * time.Second
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 500
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = 30 * time.Second
	}
	if cfg.Config.DialTimeout == 0 {
		cfg.Config.DialTimeout = 5 * time.Second
	}
	if cfg.Config.ReadTimeout == 0 {
		cfg.Config.ReadTimeout = 60 * time.Second
	}
	if cfg.Config.WriteTimeout == 0 {
		cfg.Config.WriteTimeout = 60 * time.Second
	}

	db, err := initClickHouseDB(cfg.Config) // теперь возвращает clickhouse.Conn
	if err != nil {
		return nil, err
	}
	st := &ClickhouseStorage{
		clickhouseDB: db,
		config:       cfg.Config,
		fieldnames:   cfg.FieldNames,
		tablename:    cfg.TableName,
		writeTime:    cfg.WriteTime,
		maxBatch:     cfg.MaxBatch,
		maxRetries:   cfg.MaxRetries,
		backoffBase:  cfg.BackoffBase,
		backoffMax:   cfg.BackoffMax,
		jsonFields:   make(map[string]struct{}),
	}
	for _, name := range cfg.JSONFields {
		st.jsonFields[name] = struct{}{}
	}
	go st.work()
	return st, nil
}
