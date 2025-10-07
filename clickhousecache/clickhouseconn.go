package clickhousecache

import (
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/net/context"
)

// TClickHouseConfig holds ClickHouse connection parameters
type TClickHouseConfig struct {
	ClkAddress   []string // list of clickhouse servers (host:port)
	ClkDBName    string
	ClkUserName  string
	ClkPassword  string
	MaxOpenConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Returns clickhouse.Conn (native v2 API)
func initClickHouseDB(c TClickHouseConfig) (conn clickhouse.Conn, err error) {
	if len(c.ClkAddress) == 0 {
		return nil, errors.New("you should provide at least one server address in config")
	}

	conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: c.ClkAddress,
		Auth: clickhouse.Auth{
			Database: c.ClkDBName,
			Username: c.ClkUserName,
			Password: c.ClkPassword,
		},
		// Native pooling and timeouts
		DialTimeout: c.DialTimeout,
		// Read/WriteTimeout are for query execution, not for the connection itself
		ReadTimeout: c.ReadTimeout,
		// Settings: optional, e.g. AltHosts for failover
		Settings: map[string]interface{}{
			"max_execution_time": 60,
		},
		MaxOpenConns: c.MaxOpenConns,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.DialTimeout)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}
