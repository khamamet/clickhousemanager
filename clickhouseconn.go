package clickhouseconn

import (
	"database/sql"
	"errors"
	"strings"
)

type TClickHouseConfig struct {
	ClkAddress  []string //list of clickhouse servers
	ClkUserName string
	ClkPassword string
	ClkMaxConn  int
	ClkDBName   string
	LoadSec     int
}

func initClickHouseDB(c TClickHouseConfig) (clkDB *sql.DB, err error) {
	if len(c.ClkAddress) == 0 {
		return nil, errors.New("you should provide at least one server address in config")
	}

	clkConnStr := "tcp://" + c.ClkAddress[0] + "?password=" + c.ClkPassword + "&database=" + c.ClkDBName + "&read_timeout=600&write_timeout=600"
	if c.ClkUserName != "" {
		clkConnStr += "&username=" + c.ClkUserName
	}

	if len(c.ClkAddress) > 1 {
		clkConnStr += "&alt_hosts=" + strings.Join(c.ClkAddress[1:], ",") + "&connection_open_strategy=random"
	}

	clkDB, err = sql.Open("clickhouse", clkConnStr)
	if err != nil {
		return
	}
	if err = clkDB.Ping(); err != nil {
		return
	}
	clkDB.SetMaxIdleConns(c.ClkMaxConn)
	clkDB.SetMaxOpenConns(c.ClkMaxConn)
	return
}
