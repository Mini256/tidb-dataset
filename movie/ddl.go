package movie

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

const (
	tableMovie         = "movie"
	tableUser          = "user"
	tableRating        = "rating"
	tablePerson        = "person"
	tableMovieDirector = "movie_director"
	tableMovieStar     = "movie_star"
)

var tableNames = []string{
	tableMovieStar, tableMovieDirector, tableRating, tablePerson, tableUser, tableMovie,
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
	s := getMovieState(ctx)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

// createTables creates tables schema.
func (w *ddlManager) createTables(ctx context.Context) error {
	// Movie.
	query := `
		CREATE TABLE IF NOT EXISTS movie (
			id BIGINT NOT NULL,
			title VARCHAR(128),
			type VARCHAR(20),
			year SMALLINT,
			released_at DATETIME,
			PRIMARY KEY (id)
		)
	`
	w.log.Printf("Creating table %s.\n", tableMovie)
	if err := w.execTableDDL(ctx, query); err != nil {
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

	w.log.Printf("Creating table %s.\n", tableUser)
	if err := w.execTableDDL(ctx, query); err != nil {
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

	w.log.Printf("Creating table %s.\n", tableRating)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Person.
	query = `
		CREATE TABLE IF NOT EXISTS person (
			id BIGINT NOT NULL,
			name VARCHAR(128) NOT NULL,
			gender TINYINT(1),
			birth_year SMALLINT,
			death_year SMALLINT,
			PRIMARY KEY (id)
		)
	`

	w.log.Printf("Creating table %s.\n", tablePerson)
	if err := w.execTableDDL(ctx, query); err != nil {
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

	w.log.Printf("Creating table %s.\n", tableMovieDirector)
	if err := w.execTableDDL(ctx, query); err != nil {
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

	w.log.Printf("Creating table %s.\n", tableMovieStar)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	w.log.Info("Finished creating tables!")

	return nil
}

// dropTables creates tables schema.
func (w *ddlManager) dropTables(ctx context.Context) error {
	dropTables := []string{
		tableMovieStar, tableMovieDirector, tableRating, tablePerson, tableUser, tableUser,
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
