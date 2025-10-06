package clickhousecache

import "time"

type ClickhouseStorageConfig struct {
	Config      TClickHouseConfig
	TableName   string
	FieldNames  []string
	WriteTime   time.Duration
	MaxBatch    int
	JSONFields  []string
	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// NewClickhouseStorage создает ClickhouseStorage, используя конфиг с дефолтами.
func NewClickhouseStorage(cfg ClickhouseStorageConfig) (*ClickhouseStorage, error) {
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

	db, err := initClickHouseDB(cfg.Config)
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
