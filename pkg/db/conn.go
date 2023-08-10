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

func OpenDB(dbName, host string, port int, user, password string) (*sql.DB, error) {
	var (
		err error
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
	)

	globalDB, err := sql.Open(mysqlDriver, fmt.Sprintf("%s%s?tls=preferred", dsn, dbName))
	if err != nil {
		return nil, err
	}

	// Check if it can connect to the database.
	if err := globalDB.Ping(); err != nil {
		errString := err.Error()

		// If the specified database does not exist, create one.
		if strings.Contains(errString, unknownDB) {
			var tmpDB *sql.DB
			tmpDB, openErr := sql.Open(mysqlDriver, fmt.Sprintf("%s?tls=preferred", dsn))
			if openErr != nil {
				return nil, openErr
			}
			defer func(db *sql.DB) {
				err := db.Close()
				if err != nil {
					panic(err)
				}
			}(tmpDB)
			if _, execErr := tmpDB.Exec(createDBDDL + dbName); execErr != nil {
				return nil, fmt.Errorf("failed to create database, err %v", execErr)
			}
		} else {
			globalDB = nil
			return nil, err
		}
	}

	return globalDB, nil
}
