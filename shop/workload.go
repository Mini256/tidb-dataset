package shop

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Mini256/tidb-dataset/pkg/workload"
	"github.com/sirupsen/logrus"
)

// Config is the configuration for book demo workload.
type Config struct {
	DBName     string
	DropTables bool
	OrderCount int
}

// Workloader is book demo workload.
type Workloader struct {
	db         *sql.DB
	log        *logrus.Entry
	cfg        Config
	ddlManager *ddlManager
}

type contextKey string

const stateKey = contextKey("shop")

type shopState struct {
	*workload.DatasetState
}

func getShopState(ctx context.Context) *shopState {
	s := ctx.Value(stateKey).(*shopState)
	return s
}

func NewWorkloader(db *sql.DB, cfg Config) (*Workloader, error) {
	if db == nil {
		panic(fmt.Errorf("failed to connect to database when loading data"))
	}

	logger := logrus.WithField("dataset", "shop")

	w := &Workloader{
		db:         db,
		cfg:        cfg,
		log:        logger,
		ddlManager: newDDLManager(logger),
	}

	return w, nil
}

func (w *Workloader) Name() string {
	return "shop"
}

func (w *Workloader) DBName() string {
	return w.cfg.DBName
}

// InitThread inits thread.
func (w *Workloader) InitThread(ctx context.Context) context.Context {
	s := &shopState{
		DatasetState: workload.NewDatasetState(ctx, w.db),
	}
	ctx = context.WithValue(ctx, stateKey, s)

	return ctx
}

// CleanupThread implements Workloader interface.
func (w *Workloader) CleanupThread(ctx context.Context) {
	s := getShopState(ctx)
	if s.Conn != nil {
		err := s.Conn.Close()
		if err != nil {
			w.log.Warn(
				"failed to close the database connection when cleaned up the thread",
			)
			return
		}
	}
}

// Prepare implements Workloader interface.
func (w *Workloader) Prepare(ctx context.Context) error {
	s := getShopState(ctx)

	if w.db == nil || s.Conn == nil {
		return fmt.Errorf("failed to connect the database")
	}

	// Drop the old table if it needs.
	if w.cfg.DropTables {
		w.log.Info("Dropping the old tables....")
		err := w.ddlManager.dropTables(ctx)
		if err != nil {
			return err
		}
	}

	w.log.Info("Creating the tables if not existed....")
	if err := w.ddlManager.createTables(ctx); err != nil {
		return err
	}

	w.log.Info("Clearing the old data....")
	for _, tableName := range tableNames {
		query := fmt.Sprintf("TRUNCATE TABLE %s;", tableName)
		_, err := s.Conn.ExecContext(ctx, query)
		if err != nil {
			return err
		}
	}

	return prepareWorkload(ctx, w.log, w)
}

func (w *Workloader) Run(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (w *Workloader) Cleanup(ctx context.Context) error {
	w.log.Info("Dropping the tables....")
	err := w.ddlManager.dropTables(ctx)
	if err != nil {
		return err
	}

	return nil
}
