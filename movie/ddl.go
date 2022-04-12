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
			id bigint(20) NOT NULL,
			title varchar(100) NOT NULL,
			type enum(
				'Action','Adventure','Animation','Children','Comedy','Crime',
				'Documentary','Drama','Fantasy','Film-Noir','Horror','Musical',
				'Mystery','Romance','Sci-Fi','Thriller','War'
			) NOT NULL,
			year smallint(6) NOT NULL,
			release_at datetime NOT NULL,
			PRIMARY KEY (id) CLUSTERED
		)
	`
	w.log.Printf("Creating table %s.\n", tableMovie)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// User.
	query = `
		CREATE TABLE IF NOT EXISTS user (
			id bigint NOT NULL,
			nickname varchar(100) NOT NULL,
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
			movie_id bigint NOT NULL,
			user_id bigint NOT NULL,
			score tinyint NOT NULL,
			rating_at datetime NOT NULL DEFAULT NOW() ON UPDATE NOW(),
			PRIMARY KEY (movie_id, user_id) CLUSTERED
		)
	`

	w.log.Printf("Creating table %s.\n", tableRating)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Person.
	query = `
		CREATE TABLE IF NOT EXISTS person (
			id bigint(20) NOT NULL,
			name varchar(100) NOT NULL,
			gender tinyint(1) DEFAULT NULL,
			birth_year smallint(6) DEFAULT NULL,
			death_year smallint(6) DEFAULT NULL,
			PRIMARY KEY (id) CLUSTERED
		)
	`

	w.log.Printf("Creating table %s.\n", tablePerson)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Movie Director.
	query = `
		CREATE TABLE IF NOT EXISTS movie_director (
			movie_id bigint(20) NOT NULL,
			director_id bigint(20) NOT NULL,
			PRIMARY KEY (movie_id,director_id) CLUSTERED
		)
	`

	w.log.Printf("Creating table %s.\n", tableMovieDirector)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Movie Star.
	query = `
		CREATE TABLE IF NOT EXISTS movie_star (
			movie_id bigint(20) NOT NULL,
			star_id bigint(20) NOT NULL,
			PRIMARY KEY (movie_id,star_id) CLUSTERED
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
		tableMovieStar, tableMovieDirector, tableRating, tablePerson, tableUser, tableMovie,
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
