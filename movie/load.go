package movie

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/util"
	rand "github.com/brianvoe/gofakeit/v6"
)

const (
	defaultUserCount        = 10000
	defaultPersonCount      = 20000
	defaultMovieCount       = 20000
	defaultRatingCount      = 300000
	defaultMaxStarsPerMovie = 10
)

var movieTypes = []string{
	"Action",
	"Adventure",
	"Animation",
	"Children's",
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

func (w *Workloader) loadUsers(ctx context.Context) (util.UInt32, error) {
	dml := "INSERT INTO user (id, nickname) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	userIDs := make(util.UInt32)
	userCount := defaultUserCount
	if w.cfg.UserCount != 0 {
		userCount = int(w.cfg.UserCount)
	}

	for len(userIDs) < userCount {
		userID := rand.Uint32()
		if _, ok := userIDs[userID]; ok {
			continue
		} else {
			userIDs[userID] = struct{}{}
		}
		userName := rand.Username()

		v := []string{fmt.Sprintf(`(%d, '%s')`, userID, userName)}
		if err := bl.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return userIDs, bl.Flush(ctx)
}

func (w *Workloader) loadMovies(ctx context.Context) (util.UInt32, error) {
	movieSQL := "INSERT INTO movie (id, title, type, year, release_time) VALUES "
	movieBL := db.NewSQLBatchLoader(w.db, movieSQL, 3, 10)
	movieIDs := make(util.UInt32)

	movieCount := defaultMovieCount
	if w.cfg.MovieCount != 0 {
		movieCount = int(w.cfg.MovieCount)
	}

	for len(movieIDs) < movieCount {
		movieID := rand.Uint32()
		if _, ok := movieIDs[movieID]; ok {
			continue
		} else {
			movieIDs[movieID] = struct{}{}
		}
		movieType := rand.RandomString(movieTypes)
		movieTitle := getMovieTitle(movieType)
		movieYear := rand.Year()
		movieReleaseTime := rand.DateRange(
			time.Date(movieYear, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(movieYear, 12, 31, 0, 0, 0, 0, time.UTC),
		)

		v := []string{fmt.Sprintf(`(%d, '%s', '%s', %d, '%s')`,
			movieID, movieTitle, movieType, movieYear, movieReleaseTime.Format(time.RFC3339),
		)}
		if err := movieBL.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return movieIDs, movieBL.Flush(ctx)
}

func getMovieTitle(movieType string) string {
	movieTitle := ""
	switch movieType {
	case "Children":
		movieTitle = "The Story of " + rand.PetName()
	case "Adventure":
		movieTitle = "The Adventures of " + rand.Name()
	case "Documentary":
		movieTitle = "The Documentary of " + rand.Animal()
	case "Sci-Fi":
		movieTitle = "The Phylogeny of " + rand.Company()
	case "War":
		movieTitle = "The Battle of " + rand.City()
	default:
		movieTitle = rand.Name()
	}
	return strings.ReplaceAll(movieTitle, "'", "\\'")
}

func (w *Workloader) loadPersons(ctx context.Context) (util.UInt32, error) {
	dml := "INSERT INTO person (id, name, gender, birth_year, death_year) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)
	personIDs := make(util.UInt32)

	personCount := defaultPersonCount
	if w.cfg.PersonCount != 0 {
		personCount = int(w.cfg.PersonCount)
	}

	for len(personIDs) < personCount {
		id := rand.Uint32()

		if _, exists := personIDs[id]; exists {
			continue
		} else {
			personIDs[id] = struct{}{}
		}

		name := rand.Name()
		gender := rand.IntRange(0, 1) // 0: female, 1: male
		birthYear := rand.IntRange(1930, 2000)
		age := rand.IntRange(0, 80)

		var v []string
		deathYear := birthYear + age
		if deathYear <= time.Now().Year() {
			v = append(v, fmt.Sprintf(`(%d, '%s', %d, %d, %d)`, id, name, gender, birthYear, deathYear))
		} else {
			v = append(v, fmt.Sprintf(`(%d, '%s', %d, %d, null)`, id, name, gender, birthYear))
		}

		if err := bl.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return personIDs, bl.Flush(ctx)
}

func (w *Workloader) loadMovieDirectors(ctx context.Context, movieIDs, personIds util.UInt32) error {
	personIDArr := util.UInt32Set2Arr(personIds)
	dml := "INSERT INTO movie_director (movie_id, director_id) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	for movieID := range movieIDs {
		personIndex := rand.IntRange(0, len(personIds)-1)
		personID := personIDArr[uint32(personIndex)]

		v := []string{fmt.Sprintf(`(%d, %d)`, movieID, personID)}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}

func (w *Workloader) loadMovieStars(ctx context.Context, movieIDs, personIDs util.UInt32) error {
	personIDArr := util.UInt32Set2Arr(personIDs)
	dml := "INSERT INTO movie_star (movie_id, star_id) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	maxStarsPerMovie := defaultMaxStarsPerMovie
	if w.cfg.MaxStarsPerMovie != 0 {
		maxStarsPerMovie = int(w.cfg.MaxStarsPerMovie)
	}

	for movieID := range movieIDs {
		starValues := make([]string, 0)
		starIds := make(util.UInt32)
		for i := 0; i < rand.IntRange(1, maxStarsPerMovie); i++ {
			personIndex := rand.IntRange(0, len(personIDs)-1)
			personID := personIDArr[uint32(personIndex)]

			if _, exists := starIds[personID]; exists {
				continue
			} else {
				starIds[personID] = struct{}{}
			}

			starValues = append(starValues, fmt.Sprintf(`(%d, %d)`, movieID, personID))
		}

		if err := bl.InsertValue(ctx, starValues); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}

func (w *Workloader) loadRatings(ctx context.Context, userIDs, movieIDs util.UInt32) error {
	dml := "INSERT INTO rating (movie_id, user_id, score, rating_at) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	userIDArr := util.UInt32Set2Arr(userIDs)
	movieIDArr := util.UInt32Set2Arr(movieIDs)

	ratingSet := make(util.String)
	ratingCount := defaultRatingCount
	if w.cfg.RatingCount != 0 {
		ratingCount = int(w.cfg.RatingCount)
	}

	for len(ratingSet) < ratingCount {
		movieIndex := uint32(rand.IntRange(0, len(movieIDs)-1))
		movieID := movieIDArr[movieIndex]
		userIndex := uint32(rand.IntRange(0, len(userIDs)-1))
		userID := userIDArr[userIndex]

		key := fmt.Sprintf("%d-%d", movieID, userID)
		if _, ok := ratingSet[key]; ok {
			continue
		} else {
			ratingSet[key] = struct{}{}
		}

		score := rand.IntRange(0, 5)
		ratingAt := rand.DateRange(
			time.Date(2010, 0, 0, 0, 0, 0, 0, time.UTC),
			time.Now(),
		)

		v := []string{fmt.Sprintf(`(%d, %d, %d, '%s')`, movieID, userID, score, ratingAt.String())}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}
