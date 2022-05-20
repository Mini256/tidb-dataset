package shop

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

type bookLoader interface {
	allowAutoRandomExplicitInsert(ctx context.Context) error
	loadOrdersAndExpress(ctx context.Context) error
}

func prepareWorkload(ctx context.Context, log *logrus.Entry, l bookLoader) error {
	var err error

	err = l.allowAutoRandomExplicitInsert(ctx)
	if err != nil {
		return fmt.Errorf("failed to allow auto random explicit insert: %v", err)
	}

	log.Info("Loading book orders and express data...")
	if err = l.loadOrdersAndExpress(ctx); err != nil {
		return fmt.Errorf("failed to load orders and express data: %v", err)
	}

	return nil
}
