package bookshop

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/util"
	rand "github.com/brianvoe/gofakeit/v6"
)

const (
	DefaultUserCount   = 10000
	DefaultAuthorCount = 20000
	DefaultBookCount   = 20000
	DefaultOrderCount  = 300000
	DefaultRatingCount = 300000
)

var bookTypes = []string{
	"Magazine",
	"Novel",
	"Life",
	"Arts",
	"Comics",
	"Education & Reference",
	"Humanities & Social Sciences",
	"Science & Technology",
	"Kids",
	"Sports",
}

func (w *Workloader) loadUsers(ctx context.Context) (util.UInt32, error) {
	dml := "INSERT INTO users (id, nickname, balance) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	userIDs := make(util.UInt32)
	userNicknames := make(util.String)
	userCount := DefaultUserCount
	if w.cfg.UserCount != 0 {
		userCount = int(w.cfg.UserCount)
	}

	for len(userIDs) < userCount {
		userID := uint32(rand.UintRange(1000, math.MaxInt))
		if _, ok := userIDs[userID]; ok {
			continue
		} else {
			userIDs[userID] = struct{}{}
		}

		nickname := rand.Username()
		if _, ok := userNicknames[nickname]; ok {
			continue
		} else {
			userNicknames[nickname] = struct{}{}
		}

		balance := rand.Float64Range(100, 10000)

		v := []string{fmt.Sprintf(`(%d, '%s', %f)`, userID, nickname, balance)}
		if err := bl.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return userIDs, bl.Flush(ctx)
}

func (w *Workloader) loadBooks(ctx context.Context) (util.UInt32, error) {
	bookSQL := "INSERT INTO books (id, title, type, published_at, stock, price) VALUES "
	bookBL := db.NewSQLBatchLoader(w.db, bookSQL, 3, 10)
	bookIDs := make(util.UInt32)

	bookCount := DefaultBookCount
	if w.cfg.BookCount != 0 {
		bookCount = int(w.cfg.BookCount)
	}

	for len(bookIDs) < bookCount {
		bookID := uint32(rand.UintRange(1000, math.MaxInt))
		if _, ok := bookIDs[bookID]; ok {
			continue
		} else {
			bookIDs[bookID] = struct{}{}
		}
		bookType := rand.RandomString(bookTypes)
		bookTitle := getBookTitle(bookType)
		bookReleaseTime := rand.DateRange(
			time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(time.Now().Year(), 12, 31, 0, 0, 0, 0, time.UTC),
		)
		stock := rand.IntRange(10, 1000)
		price := rand.Float64Range(10, 500)

		v := []string{fmt.Sprintf(`(%d, '%s', '%s', '%s', %d, %f)`,
			bookID, bookTitle, bookType, bookReleaseTime.Format(time.RFC3339), stock, price,
		)}
		if err := bookBL.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return bookIDs, bookBL.Flush(ctx)
}

func getBookTitle(bookType string) string {
	bookTitle := ""
	switch bookType {
	case "Novel":
		bookTitle = "The Story of " + rand.PetName()
	case "Comics":
		bookTitle = "The Adventures of " + rand.Name()
	case "Magazine":
		bookTitle = "The Documentary of " + rand.Animal()
	case "Humanities & Social Sciences":
		bookTitle = "The History of " + rand.Company()
	default:
		bookTitle = rand.Name()
	}
	return strings.ReplaceAll(bookTitle, "'", "\\'")
}

func (w *Workloader) loadAuthors(ctx context.Context) (util.UInt32, error) {
	dml := "INSERT INTO authors (id, name, gender, birth_year, death_year) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)
	authorIDs := make(util.UInt32)

	authorCount := DefaultAuthorCount
	if w.cfg.AuthorCount != 0 {
		authorCount = int(w.cfg.AuthorCount)
	}

	for len(authorIDs) < authorCount {
		authorID := uint32(rand.UintRange(1000, math.MaxInt))

		if _, exists := authorIDs[authorID]; exists {
			continue
		} else {
			authorIDs[authorID] = struct{}{}
		}

		name := rand.Name()
		gender := rand.IntRange(0, 1) // 0: female, 1: male
		birthYear := rand.IntRange(1930, 2000)
		age := rand.IntRange(0, 80)

		var v []string
		deathYear := birthYear + age
		if deathYear <= time.Now().Year() {
			v = append(v, fmt.Sprintf(`(%d, '%s', %d, %d, %d)`, authorID, name, gender, birthYear, deathYear))
		} else {
			v = append(v, fmt.Sprintf(`(%d, '%s', %d, %d, null)`, authorID, name, gender, birthYear))
		}

		if err := bl.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return authorIDs, bl.Flush(ctx)
}

func (w *Workloader) loadBookAuthors(ctx context.Context, bookIDs, authorIds util.UInt32) error {
	authorIDArr := util.UInt32Set2Arr(authorIds)
	dml := "INSERT INTO book_authors (book_id, author_id) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	for bookID := range bookIDs {
		authorIndex := rand.IntRange(0, len(authorIds)-1)
		authorID := authorIDArr[uint32(authorIndex)]

		v := []string{fmt.Sprintf(`(%d, %d)`, bookID, authorID)}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}

func (w *Workloader) loadOrders(ctx context.Context, userIDs, bookIDs util.UInt32) error {
	dml := "INSERT INTO orders (id, book_id, user_id, quality, ordered_at) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	userIDArr := util.UInt32Set2Arr(userIDs)
	bookIDArr := util.UInt32Set2Arr(bookIDs)

	orderSet := make(util.UInt32)
	orderCount := DefaultOrderCount
	if w.cfg.OrderCount != 0 {
		orderCount = int(w.cfg.OrderCount)
	}

	for len(orderSet) < orderCount {
		orderID := uint32(rand.UintRange(1000, math.MaxInt))
		if _, ok := orderSet[orderID]; ok {
			continue
		} else {
			orderSet[orderID] = struct{}{}
		}

		bookIndex := uint32(rand.IntRange(0, len(bookIDs)-1))
		bookID := bookIDArr[bookIndex]
		userIndex := uint32(rand.IntRange(0, len(userIDs)-1))
		userID := userIDArr[userIndex]
		quality := rand.IntRange(1, 10)
		ratedAt := rand.DateRange(
			time.Date(2010, 0, 0, 0, 0, 0, 0, time.UTC),
			time.Now(),
		)

		v := []string{fmt.Sprintf(`(%d, %d, %d, %d, '%s')`, orderID, bookID, userID, quality, ratedAt.Format(time.RFC3339))}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}

func (w *Workloader) loadRatings(ctx context.Context, userIDs, bookIDs util.UInt32) error {
	dml := "INSERT INTO ratings (book_id, user_id, score, rated_at) VALUES "
	bl := db.NewSQLBatchLoader(w.db, dml, 3, 10)

	userIDArr := util.UInt32Set2Arr(userIDs)
	bookIDArr := util.UInt32Set2Arr(bookIDs)

	ratingSet := make(util.String)
	ratingCount := DefaultRatingCount
	if w.cfg.RatingCount != 0 {
		ratingCount = int(w.cfg.RatingCount)
	}

	for len(ratingSet) < ratingCount {
		bookIndex := uint32(rand.IntRange(0, len(bookIDs)-1))
		bookID := bookIDArr[bookIndex]
		userIndex := uint32(rand.IntRange(0, len(userIDs)-1))
		userID := userIDArr[userIndex]

		key := fmt.Sprintf("%d-%d", bookID, userID)
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

		v := []string{fmt.Sprintf(`(%d, %d, %d, '%s')`, bookID, userID, score, ratingAt.String())}
		if err := bl.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return bl.Flush(ctx)
}
