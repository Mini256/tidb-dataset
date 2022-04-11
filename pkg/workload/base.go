package workload

import (
	"context"
	"database/sql"
)

// DatasetState saves state for each thread
type DatasetState struct {
	DB   *sql.DB
	Conn *sql.Conn
}

func (t *DatasetState) RefreshConn(ctx context.Context) error {
	conn, err := t.DB.Conn(ctx)
	if err != nil {
		return err
	}
	t.Conn = conn
	return nil
}

// NewDatasetState creates a base DatasetState
func NewDatasetState(ctx context.Context, db *sql.DB) *DatasetState {
	var conn *sql.Conn
	var err error
	if db != nil {
		conn, err = db.Conn(ctx)
		if err != nil {
			panic(err.Error())
		}
	}

	s := &DatasetState{
		DB:   db,
		Conn: conn,
	}
	return s
}
