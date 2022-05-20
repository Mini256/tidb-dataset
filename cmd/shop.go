package main

import (
	"database/sql"
	"fmt"

	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/workload"
	"github.com/Mini256/tidb-dataset/shop"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const DefaultShopDBName = "shop"

var shopCfg shop.Config

func executeShop(action string) error {
	log := logrus.WithField("dataset", "shop")

	var (
		globalDB *sql.DB
		err      error
	)

	// Init database connection.
	globalDB, err = db.OpenDB(shopCfg.DBName, host, port, user, password)
	if err != nil {
		db.CloseDB(globalDB)
		log.WithError(err).Errorf("cannot open database, please check it (ip/port/username/password)")
		return nil
	}
	defer db.CloseDB(globalDB)

	// Init context state for current thread.
	var w workload.Workloader
	w, err = shop.NewWorkloader(globalDB, shopCfg)
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

func registerShop(root *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "shop",
		Short: "A dataset about a virtual online shop.",
	}
	cmd.PersistentFlags().StringVarP(&shopCfg.DBName, "db", "D", DefaultShopDBName, "Database name")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare test data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeShop("prepare")
		},
	}

	cmdPrepare.PersistentFlags().BoolVar(&shopCfg.DropTables, "drop-tables", false,
		"Drop the tables before prepare")
	cmdPrepare.PersistentFlags().IntVar(&shopCfg.OrderCount, "orders", shop.DefaultOrderCount,
		"Specify the number of orders")

	var cmdCleanUp = &cobra.Command{
		Use:   "cleanup",
		Short: "Clean up test data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeShop("cleanup")
		},
	}

	cmd.AddCommand(cmdPrepare)
	cmd.AddCommand(cmdCleanUp)

	root.AddCommand(cmd)
}
