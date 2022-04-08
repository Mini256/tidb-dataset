package main

import (
	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/movie"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var cfg movie.Config

func executeMovie() error {
	log := logrus.WithField("dataset", "movie")

	// Init database connection.
	globalDB, err := db.OpenDB(dbName, host, port, user, password, threads, acThreads)
	if err != nil {
		log.Error("Cannot open database, please check it (ip/port/username/password)")
		db.CloseDB(globalDB)
		return err
	}
	defer db.CloseDB(globalDB)

	conn, err := globalDB.Conn(globalCtx)
	if err != nil {
		log.WithError(err).Error("Failed to get connection for db.")
		return err
	}

	// Init context.
	loader := movie.NewLoader(globalDB, cfg)

	// Execute.
	err = loader.LoadDataset(globalCtx, conn, log)
	if err != nil {
		log.WithError(err).Error("Failed to load dataset.")
		return err
	}

	log.Info("Finished!")

	return nil
}

func registerMovie(root *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "movie",
		Short: "A dataset about movies.",
	}

	var cmdRun = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare test data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeMovie()
		},
	}

	cmdRun.PersistentFlags().BoolVar(&cfg.DropTables, "drop-tables", false, "Drop the tables before prepare")

	cmd.AddCommand(cmdRun)
	root.AddCommand(cmd)
}
