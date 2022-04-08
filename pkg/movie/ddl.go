package movie

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	tableMovie         = "movie"
	tableUser          = "user"
	tableRating        = "rating"
	tablePerson        = "person"
	tableMovieType     = "movie_type"
	tableMovieDirector = "movie_director"
	tableMovieStar     = "movie_star"
)

var tableNames = []string{
	tableMovieStar, tableMovieDirector, tableMovieType, tableRating, tablePerson, tableUser, tableUser,
}

type ddlManager struct {
}

func newDDLManager() *ddlManager {
	return &ddlManager{}
}

func (w *ddlManager) execTableDDL(ctx context.Context, conn *sql.Conn, query string) error {
	if _, err := conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

// createTables creates tables schema.
func (w *ddlManager) createTables(ctx context.Context, conn *sql.Conn) error {
	// Movie.
	query := `
		CREATE TABLE IF NOT EXISTS movie (
			id BIGINT NOT NULL,
			title VARCHAR(128),
			year SMALLINT,
			released_at DATETIME,
			PRIMARY KEY (id)
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	// User.
	query = `
		CREATE TABLE IF NOT EXISTS user (
			id BIGINT NOT NULL,
			username VARCHAR(128) NOT NULL,
			PRIMARY KEY (id)
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	// Rating.
	query = `
		CREATE TABLE IF NOT EXISTS rating (
			movie_id BIGINT NOT NULL,
			user_id BIGINT NOT NULL,
			score TINYINT NOT NULL,
			rating_at TIMESTAMP NOT NULL,
			PRIMARY KEY (movie_id, user_id)
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	// Person.
	query = `
		CREATE TABLE IF NOT EXISTS person (
			id BIGINT NOT NULL,
			name VARCHAR(128) NOT NULL,
			birth_year SMALLINT,
			death_year SMALLINT,
			PRIMARY KEY (id)
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	// Movie Type.
	query = `
		CREATE TABLE IF NOT EXISTS movie_type (
			movie_id BIGINT NOT NULL,
			type CHAR(20) NOT NULL
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	// Movie Director.
	query = `
		CREATE TABLE IF NOT EXISTS movie_director (
			movie_id BIGINT NOT NULL,
			director_id BIGINT NOT NULL,
			PRIMARY KEY (movie_id, director_id)
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	// Movie Star.
	query = `
		CREATE TABLE IF NOT EXISTS movie_star (
			movie_id BIGINT NOT NULL,
			star_id BIGINT NOT NULL,
			PRIMARY KEY (movie_id, star_id)
		)
	`

	if err := w.execTableDDL(ctx, conn, query); err != nil {
		return err
	}

	return nil
}

// dropTables creates tables schema.
func (w *ddlManager) dropTables(ctx context.Context, conn *sql.Conn) error {
	dropTables := []string{
		tableMovieStar, tableMovieDirector, tableMovieType, tableRating, tablePerson, tableUser, tableUser,
	}

	for _, table := range dropTables {
		query := fmt.Sprintf("DROP TABLE IF NOT EXISTS %s;", table)
		if err := w.execTableDDL(ctx, conn, query); err != nil {
			return err
		}
	}

	return nil
}
