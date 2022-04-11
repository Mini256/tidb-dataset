package movie

import (
	"context"
	"fmt"

	"github.com/Mini256/tidb-dataset/pkg/util"
	"github.com/sirupsen/logrus"
)

type movieLoader interface {
	loadUsers(ctx context.Context) (util.UInt32, error)
	loadMovies(ctx context.Context) (util.UInt32, error)
	loadPersons(ctx context.Context) (util.UInt32, error)
	loadMovieDirectors(ctx context.Context, movieIds, personIds util.UInt32) error
	loadMovieStars(ctx context.Context, movieIds, personIds util.UInt32) error
	loadRatings(ctx context.Context, userIds, movieIds util.UInt32) error
}

func prepareWorkload(ctx context.Context, log *logrus.Entry, l movieLoader) error {
	var err error

	var userIds util.UInt32
	log.Info("Loading users data...")
	if userIds, err = l.loadUsers(ctx); err != nil {
		return fmt.Errorf("failed to load users data: %v", err)
	}

	var movieIds util.UInt32
	log.Info("Loading movies data...")
	if movieIds, err = l.loadMovies(ctx); err != nil {
		return fmt.Errorf("failed to load movies data: %v", err)
	}

	var personIds util.UInt32
	log.Info("Loading persons data...")
	if personIds, err = l.loadPersons(ctx); err != nil {
		return fmt.Errorf("failed to load persons data: %v", err)
	}

	log.Info("Loading movie directors data...")
	if err = l.loadMovieDirectors(ctx, movieIds, personIds); err != nil {
		return fmt.Errorf("failed to load movie directors data: %v", err)
	}

	log.Info("Loading movie stars data...")
	if err = l.loadMovieStars(ctx, movieIds, personIds); err != nil {
		return fmt.Errorf("failed to load movie stars data: %v", err)
	}

	log.Info("Loading movie ratings data...")
	if err = l.loadRatings(ctx, userIds, movieIds); err != nil {
		return fmt.Errorf("failed to load ratings data: %v", err)
	}

	return nil
}
