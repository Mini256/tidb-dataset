package bookshop

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

const (
	tableBooks       = "books"
	tableUsers       = "users"
	tableAuthors     = "authors"
	tableBookAuthors = "book_authors"
	tableOrders      = "orders"
	tableRatings     = "ratings"
)

var tableNames = []string{
	tableOrders, tableRatings, tableBookAuthors, tableAuthors,
	tableUsers, tableBooks,
}

type ddlManager struct {
	log *logrus.Entry
}

func newDDLManager(log *logrus.Entry) *ddlManager {
	return &ddlManager{
		log: log,
	}
}

func (w *ddlManager) execTableDDL(ctx context.Context, query string) error {
	s := getBookState(ctx)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

// createTables creates tables schema.
func (w *ddlManager) createTables(ctx context.Context) error {
	// Books.
	query := `
		CREATE TABLE IF NOT EXISTS books (
			id bigint(20) NOT NULL,
			title varchar(100) NOT NULL,
			type enum('Magazine', 'Novel', 'Life', 'Arts', 'Comics', 'Education & Reference', 
				'Humanities & Social Sciences', 'Science & Technology', 'Kids', 'Sports') NOT NULL,
			published_at datetime NOT NULL,
			stock int(11) DEFAULT '0',
			price decimal(15,2) DEFAULT '0.0',
			PRIMARY KEY (id) CLUSTERED
		) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
	`
	w.log.Printf("Creating table %s.\n", tableBooks)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Users.
	query = `
		CREATE TABLE IF NOT EXISTS users (
			id bigint NOT NULL,
			balance decimal(15,2) DEFAULT '0.0',
			nickname varchar(100) UNIQUE NOT NULL,
			PRIMARY KEY (id) NONCLUSTERED
		) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
	`

	w.log.Printf("Creating table %s.\n", tableUsers)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Authors.
	query = `
		CREATE TABLE IF NOT EXISTS authors (
			id bigint(20) NOT NULL,
			name varchar(100) NOT NULL,
			gender tinyint(1) DEFAULT NULL,
			birth_year smallint(6) DEFAULT NULL,
			death_year smallint(6) DEFAULT NULL,
			PRIMARY KEY (id) CLUSTERED
		) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
	`

	w.log.Printf("Creating table %s.\n", tableAuthors)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Book Authors.
	query = `
		CREATE TABLE IF NOT EXISTS book_authors (
			book_id bigint(20) NOT NULL,
			author_id bigint(20) NOT NULL,
			PRIMARY KEY (book_id, author_id) CLUSTERED
		) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
	`

	w.log.Printf("Creating table %s.\n", tableBookAuthors)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Orders.
	query = `
		CREATE TABLE IF NOT EXISTS orders (
			id bigint(20) NOT NULL,
			book_id bigint(20) NOT NULL,
			user_id bigint(20) NOT NULL,
			quality tinyint(4) NOT NULL,
			ordered_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (id) CLUSTERED,
			KEY orders_book_id_idx (book_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
	`

	w.log.Printf("Creating table %s.\n", tableOrders)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Ratings.
	query = `
	CREATE TABLE IF NOT EXISTS ratings (
		book_id bigint NOT NULL,
		user_id bigint NOT NULL,
		score tinyint NOT NULL,
		rated_at datetime NOT NULL DEFAULT NOW() ON UPDATE NOW(),
		PRIMARY KEY (book_id, user_id) CLUSTERED,
		UNIQUE KEY uniq_book_user_idx (book_id, user_id)
	) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
	`

	w.log.Printf("Creating table %s.\n", tableRatings)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	w.log.Info("Finished creating tables!")

	return nil
}

// dropTables creates tables schema.
func (w *ddlManager) dropTables(ctx context.Context) error {
	dropTables := []string{
		tableOrders, tableRatings, tableBookAuthors, tableAuthors, tableUsers, tableBooks,
	}

	for _, tableName := range dropTables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName)
		w.log.Printf("Dropping table %s.\n", tableName)
		if err := w.execTableDDL(ctx, query); err != nil {
			return err
		}
	}

	return nil
}
