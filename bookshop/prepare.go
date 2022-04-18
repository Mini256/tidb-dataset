package bookshop

import (
	"context"
	"fmt"

	"github.com/Mini256/tidb-dataset/pkg/util"
	"github.com/sirupsen/logrus"
)

type bookLoader interface {
	loadUsers(ctx context.Context) (util.UInt32, error)
	loadBooks(ctx context.Context) (util.UInt32, error)
	loadAuthors(ctx context.Context) (util.UInt32, error)
	loadBookAuthors(ctx context.Context, bookIds, authorIds util.UInt32) error
	loadOrders(ctx context.Context, userIds, bookIds util.UInt32) error
	loadRatings(ctx context.Context, userIds, bookIds util.UInt32) error
}

func prepareWorkload(ctx context.Context, log *logrus.Entry, l bookLoader) error {
	var err error

	var userIds util.UInt32
	log.Info("Loading users data...")
	if userIds, err = l.loadUsers(ctx); err != nil {
		return fmt.Errorf("failed to load users data: %v", err)
	}

	var bookIds util.UInt32
	log.Info("Loading books data...")
	if bookIds, err = l.loadBooks(ctx); err != nil {
		return fmt.Errorf("failed to load books data: %v", err)
	}

	var authorIds util.UInt32
	log.Info("Loading authors data...")
	if authorIds, err = l.loadAuthors(ctx); err != nil {
		return fmt.Errorf("failed to load authors data: %v", err)
	}

	log.Info("Loading book authors data...")
	if err = l.loadBookAuthors(ctx, bookIds, authorIds); err != nil {
		return fmt.Errorf("failed to load book authors data: %v", err)
	}

	log.Info("Loading book orders data...")
	if err = l.loadOrders(ctx, userIds, bookIds); err != nil {
		return fmt.Errorf("failed to load orders data: %v", err)
	}

	log.Info("Loading book ratings data...")
	if err = l.loadRatings(ctx, userIds, bookIds); err != nil {
		return fmt.Errorf("failed to load ratings data: %v", err)
	}

	return nil
}
