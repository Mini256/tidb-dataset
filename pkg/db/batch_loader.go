package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	maxBatchCount = 1024
)

type BatchLoader interface {
	InsertValue(ctx context.Context, query []string) error
	Flush(ctx context.Context) error
}

// SQLBatchLoader helps us insert in batch
type SQLBatchLoader struct {
	insertHint string
	db         *sql.DB
	buf        bytes.Buffer
	count      int

	// loader retry
	retryCount    int
	retryInterval time.Duration
}

// NewSQLBatchLoader creates a batch loader for database connection
func NewSQLBatchLoader(db *sql.DB, hint string, retryCount int, retryInterval time.Duration) *SQLBatchLoader {
	return &SQLBatchLoader{
		count:         0,
		insertHint:    hint,
		db:            db,
		retryCount:    retryCount,
		retryInterval: retryInterval,
	}
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *SQLBatchLoader) InsertValue(ctx context.Context, query []string) error {
	sep := ", "
	if b.count == 0 {
		b.buf.WriteString(b.insertHint)
		sep = " "
	}
	b.buf.WriteString(sep)
	b.buf.WriteString(query[0])

	b.count++

	if b.count >= maxBatchCount {
		return b.Flush(ctx)
	}

	return nil
}

// Flush inserts all pending values
func (b *SQLBatchLoader) Flush(ctx context.Context) error {
	if b.buf.Len() == 0 {
		return nil
	}

	var err error
	for i := 0; i < 1+b.retryCount; i++ {
		_, err = b.db.ExecContext(ctx, b.buf.String())
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "Error 1062: Duplicate entry") {
			if i == 0 {
				return fmt.Errorf("exec statement error: %v", err)
			}
			break
		}
		if i < b.retryCount {
			fmt.Printf("exec statement error: %v, may try again later...\n", err)
			time.Sleep(b.retryInterval)
		}
	}
	b.count = 0
	b.buf.Reset()

	return nil
}
