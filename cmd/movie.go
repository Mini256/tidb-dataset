package main

import (
	"database/sql"
	"fmt"

	"github.com/Mini256/tidb-dataset/movie"
	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/workload"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var cfg movie.Config

func executeMovie(action string) error {
	log := logrus.WithField("dataset", "movie")

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
	w, err = movie.NewWorkloader(globalDB, cfg)
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

func registerMovie(root *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "movie",
		Short: "A dataset about movies.",
	}

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare test data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeMovie("prepare")
		},
	}

	cmdPrepare.PersistentFlags().BoolVar(&cfg.DropTables, "drop-tables", false, "Drop the tables before prepare")
	cmdPrepare.PersistentFlags().UintVar(&cfg.UserCount, "users", 10000, "Specify the number of users")
	cmdPrepare.PersistentFlags().UintVar(&cfg.PersonCount, "persons", 20000, "Specify the number of persons")
	cmdPrepare.PersistentFlags().UintVar(&cfg.MovieCount, "movies", 20000, "Specify the number of movies")
	cmdPrepare.PersistentFlags().UintVar(&cfg.RatingCount, "ratings", 300000, "Specify the number of ratings")
	cmdPrepare.PersistentFlags().UintVar(&cfg.MaxStarsPerMovie, "max-stars-per-movie", 10,
		"Specify the max number of stars of one movie")

	cmd.AddCommand(cmdPrepare)
	root.AddCommand(cmd)
}
