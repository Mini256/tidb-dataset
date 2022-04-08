package db

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	unknownDB   = "Unknown database"
	createDBDDL = "CREATE DATABASE IF NOT EXISTS "
	mysqlDriver = "mysql"
)

func CloseDB(globalDB *sql.DB) {
	if globalDB != nil {
		globalDB.Close()
	}
	globalDB = nil
}

func OpenDB(dbName, host string, port int, user, password string, threads, acThreads int) (*sql.DB, error) {
	var (
		tmpDB *sql.DB
		err   error
		ds    = fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
	)

	globalDB, err := sql.Open(mysqlDriver, fmt.Sprintf("%s%s", ds, dbName))
	if err != nil {
		return nil, err
	}
	if err := globalDB.Ping(); err != nil {
		errString := err.Error()
		if strings.Contains(errString, unknownDB) {
			tmpDB, _ = sql.Open(mysqlDriver, ds)
			defer tmpDB.Close()
			if _, err := tmpDB.Exec(createDBDDL + dbName); err != nil {
				return nil, fmt.Errorf("failed to create database, err %v", err)
			}
		} else {
			globalDB = nil
			return nil, err
		}
	} else {
		globalDB.SetMaxIdleConns(threads + acThreads + 1)
	}

	return globalDB, nil
}
