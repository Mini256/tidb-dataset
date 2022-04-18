package main

import (
	"database/sql"
	"fmt"

	"github.com/Mini256/tidb-dataset/bookshop"
	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/workload"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var cfg bookshop.Config

func executeBookshop(action string) error {
	log := logrus.WithField("dataset", "bookshop")

	var (
		globalDB *sql.DB
		err      error
	)

	// Init database connection.
	globalDB, err = db.OpenDB(dbName, host, port, user, password)
	if err != nil {
		db.CloseDB(globalDB)
		log.WithError(err).Errorf("cannot open database, please check it (ip/port/username/password)")
		return nil
	}
	defer db.CloseDB(globalDB)

	// Init context state for current thread.
	var w workload.Workloader
	w, err = bookshop.NewWorkloader(globalDB, cfg)
	if err != nil {
		panic(fmt.Errorf("failed to init work loader: %v", err))
	}

	workerCtx := w.InitThread(globalCtx)
	switch action {
	case "prepare":
		err := w.Prepare(workerCtx)
		if err != nil {
			panic(fmt.Errorf("failed to execute prepare command: %v", err))
		}
	case "cleanup":
		err := w.Cleanup(workerCtx)
		if err != nil {
			panic(fmt.Errorf("failed to execute cleanup command: %v", err))
		}
	}
	w.CleanupThread(workerCtx)

	log.Info("Finished!")

	return nil
}

func registerBookshop(root *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "bookshop",
		Short: "A dataset about a virtual online bookshop.",
	}

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare test data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeBookshop("prepare")
		},
	}

	cmdPrepare.PersistentFlags().BoolVar(&cfg.DropTables, "drop-tables", false,
		"Drop the tables before prepare")
	cmdPrepare.PersistentFlags().UintVar(&cfg.UserCount, "users", bookshop.DefaultUserCount,
		"Specify the number of users")
	cmdPrepare.PersistentFlags().UintVar(&cfg.AuthorCount, "authors", bookshop.DefaultAuthorCount,
		"Specify the number of authors")
	cmdPrepare.PersistentFlags().UintVar(&cfg.BookCount, "books", bookshop.DefaultBookCount,
		"Specify the number of books")
	cmdPrepare.PersistentFlags().UintVar(&cfg.OrderCount, "orders", bookshop.DefaultOrderCount,
		"Specify the number of orders")
	cmdPrepare.PersistentFlags().UintVar(&cfg.RatingCount, "ratings", bookshop.DefaultRatingCount,
		"Specify the number of ratings")

	var cmdCleanUp = &cobra.Command{
		Use:   "cleanup",
		Short: "Clean up test data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeBookshop("cleanup")
		},
	}

	cmd.AddCommand(cmdPrepare)
	cmd.AddCommand(cmdCleanUp)

	root.AddCommand(cmd)
}
