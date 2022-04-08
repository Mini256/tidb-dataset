package movie

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Mini256/tidb-dataset/pkg/db"
	rand "github.com/brianvoe/gofakeit/v6"
	"github.com/sirupsen/logrus"
)

const userCount = 10000
const personCount = 20000
const movieCount = 20000
const ratingCount = 300000

var movieTypes = []string{
	"Action",
	"Adventure",
	"Animation",
	"Children",
	"Comedy",
	"Crime",
	"Documentary",
	"Drama",
	"Fantasy",
	"Film-Noir",
	"Horror",
	"Musical",
	"Mystery",
	"Romance",
	"Sci-Fi",
	"Thriller",
	"War",
}

type uint32Set map[uint32]struct{}

type stringSet map[string]struct{}

type Config struct {
	DropTables bool
}

type Loader struct {
	db  *sql.DB
	cfg Config
}

func NewLoader(db *sql.DB, cfg Config) *Loader {
	return &Loader{
		db:  db,
		cfg: cfg,
	}
}

func (l *Loader) LoadDataset(ctx context.Context, conn *sql.Conn, log *logrus.Entry) error {
	ddlManager := newDDLManager()

	// Drop the old table if it needs.
	if l.cfg.DropTables {
		err := ddlManager.dropTables(ctx, conn)
		if err != nil {
			log.WithError(err).Error("Failed to drop old tables. ")
			return err
		}
	}

	// Init the table structure.
	log.Info("Creating the tables....")
	err := ddlManager.createTables(ctx, conn)
	if err != nil {
		log.WithError(err).Error("Failed to create tables. ")
		return err
	}

	// Clear old data.
	log.Info("Clearing the old data....")
	for _, tableName := range tableNames {
		s := fmt.Sprintf("truncate table %s;", tableName)
		_, err := conn.ExecContext(ctx, s)
		if err != nil {
			log.WithError(err).Error("Failed to clear old data. ")
			return err
		}
	}

	// Prepare test data.
	var userIds uint32Set
	userIds, err = l.loadUsers(ctx, log)
	if err != nil {
		log.WithError(err).Error("Failed to load users data. ")
		return err
	}

	var movieIds uint32Set
	movieIds, err = l.loadMovies(ctx, log)
	if err != nil {
		log.WithError(err).Error("Failed to load movies data. ")
		return err
	}

	var personIds uint32Set
	personIds, err = l.loadPersons(ctx, log)
	if err != nil {
		log.WithError(err).Error("Failed to load persons data. ")
		return err
	}

	err = l.loadMovieDirectors(ctx, log, movieIds, personIds)
	if err != nil {
		log.WithError(err).Error("Failed to load movie directors data. ")
		return err
	}

	err = l.loadMovieStars(ctx, log, movieIds, personIds)
	if err != nil {
		log.WithError(err).Error("Failed to load movie stars data. ")
		return err
	}

	err = l.loadRatings(ctx, log, userIds, movieIds)
	if err != nil {
		log.WithError(err).Error("Failed to load ratings data. ")
		return err
	}

	return nil
}

func (l *Loader) loadUsers(ctx context.Context, log *logrus.Entry) (uint32Set, error) {
	log.Info("Loading users....")
	s := "INSERT INTO user (id, username) VALUES "

	bl := db.NewSQLBatchLoader(l.db, s, 3, 10)
	userIds := make(uint32Set, 0)

	for len(userIds) < userCount {
		userId := rand.Uint32()
		if _, ok := userIds[userId]; ok {
			continue
		} else {
			userIds[userId] = struct{}{}
		}
		userName := rand.Username()

		v := []string{fmt.Sprintf(`(%d, '%s')`, userId, userName)}
		if err := bl.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return userIds, bl.Flush(ctx)
}

func (l *Loader) loadMovies(ctx context.Context, log *logrus.Entry) (uint32Set, error) {
	log.Info("Loading movies....")
	movieSQL := "INSERT INTO movie (id, title, year, released_at) VALUES "
	movieTypeSQL := "INSERT INTO movie_type (movie_id, type) VALUES "

	movieBL := db.NewSQLBatchLoader(l.db, movieSQL, 3, 10)
	movieTypeBL := db.NewSQLBatchLoader(l.db, movieTypeSQL, 3, 10)

	movieIds := make(uint32Set, 0)

	for len(movieIds) < movieCount {
		movieId := rand.Uint32()
		if _, ok := movieIds[movieId]; ok {
			continue
		} else {
			movieIds[movieId] = struct{}{}
		}
		movieTitle := rand.Verb()
		movieYear := rand.Year()
		movieReleaseTime := rand.DateRange(
			time.Date(movieYear, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(movieYear, 12, 31, 0, 0, 0, 0, time.UTC),
		)

		v := []string{fmt.Sprintf(`(%d, '%s', %d, '%s')`, movieId, movieTitle, movieYear, movieReleaseTime.String())}
		if err := movieBL.InsertValue(ctx, v); err != nil {
			return nil, err
		}

		movieType := rand.RandomString(movieTypes)
		typeValues := []string{fmt.Sprintf(`(%d, '%s')`, movieId, movieType)}
		if err := movieTypeBL.InsertValue(ctx, typeValues); err != nil {
			return nil, err
		}
	}

	return movieIds, movieBL.Flush(ctx)
}

func (l *Loader) loadPersons(ctx context.Context, log *logrus.Entry) (uint32Set, error) {
	log.Info("Loading person....")
	s := "INSERT INTO person (id, name, birth_year, death_year) VALUES "

	bl := db.NewSQLBatchLoader(l.db, s, 3, 10)
	personIds := make(uint32Set, 0)

	for len(personIds) < personCount {
		id := rand.Uint32()

		if _, exists := personIds[id]; exists {
			continue
		} else {
			personIds[id] = struct{}{}
		}

		name := rand.Name()
		birthYear := rand.IntRange(1950, 2000)
		age := rand.IntRange(0, 20)

		var v []string
		deathYear := birthYear + age
		if deathYear < time.Now().Year() {
			v = append(v, fmt.Sprintf(`(%d, '%s', %d, %d)`, id, name, birthYear, deathYear))
		} else {
			v = append(v, fmt.Sprintf(`(%d, '%s', %d, null)`, id, name, birthYear))
		}

		if err := bl.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return personIds, bl.Flush(ctx)
}

func (l *Loader) loadMovieDirectors(ctx context.Context, log *logrus.Entry, movieIds, personIds uint32Set) error {
	personIdArr := make([]uint32, 0, len(personIds))
	for personId, _ := range personIds {
		personIdArr = append(personIdArr, personId)
	}

	log.Info("Loading movie directors....")
	s := "INSERT INTO movie_director (movie_id, director_id) VALUES "

	bl := db.NewSQLBatchLoader(l.db, s, 3, 10)

	for movieId, _ := range movieIds {
		personIndex := rand.IntRange(0, len(personIds)-1)
		personId := personIdArr[uint32(personIndex)]

		v := []string{fmt.Sprintf(`(%d, %d)`, movieId, personId)}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}

func (l *Loader) loadMovieStars(ctx context.Context, log *logrus.Entry, movieIds, personIds uint32Set) error {
	personIdArr := make([]uint32, 0, len(personIds))
	for personId, _ := range personIds {
		personIdArr = append(personIdArr, personId)
	}

	log.Info("Loading movie stars....")
	s := "INSERT INTO movie_star (movie_id, star_id) VALUES "

	bl := db.NewSQLBatchLoader(l.db, s, 3, 10)

	for movieId := range movieIds {
		starValues := make([]string, 0)
		starIds := make(uint32Set, 0)
		for i := 0; i < rand.IntRange(1, 10); i++ {
			personIndex := rand.IntRange(0, len(personIds)-1)
			personId := personIdArr[uint32(personIndex)]

			if _, exists := starIds[personId]; exists {
				continue
			} else {
				starIds[personId] = struct{}{}
			}

			starValues = append(starValues, fmt.Sprintf(`(%d, %d)`, movieId, personId))
		}

		if err := bl.InsertValue(ctx, starValues); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}

func (l *Loader) loadRatings(ctx context.Context, log *logrus.Entry, userIds, movieIds uint32Set) error {
	log.Info("Loading ratings....")
	s := "INSERT INTO rating (movie_id, user_id, score, rating_at) VALUES "

	bl := db.NewSQLBatchLoader(l.db, s, 3, 10)

	userIdArr := make([]uint32, 0, len(userIds))
	for userId, _ := range userIds {
		userIdArr = append(userIdArr, userId)
	}

	movieIdArr := make([]uint32, 0, len(movieIds))
	for movieId, _ := range movieIds {
		movieIdArr = append(movieIdArr, movieId)
	}

	ratingSet := make(stringSet, 0)
	for len(ratingSet) < ratingCount {
		movieIndex := uint32(rand.IntRange(0, len(movieIds)-1))
		movieId := movieIdArr[movieIndex]
		userIndex := uint32(rand.IntRange(0, len(userIds)-1))
		userId := userIdArr[userIndex]

		key := fmt.Sprintf("%d-%d", movieId, userId)
		if _, ok := ratingSet[key]; ok {
			continue
		} else {
			ratingSet[key] = struct{}{}
		}

		score := rand.IntRange(0, 5)

		ratingAt := rand.DateRange(
			time.Date(2000, 0, 0, 0, 0, 0, 0, time.UTC),
			time.Now(),
		)

		v := []string{fmt.Sprintf(`(%d, %d, %d, '%s')`, movieId, userId, score, ratingAt.String())}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}
