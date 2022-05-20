package shop

import (
	"context"
	"fmt"
	"github.com/Mini256/tidb-dataset/pkg/db"
	"github.com/Mini256/tidb-dataset/pkg/util"
	rand "github.com/brianvoe/gofakeit/v6"
	"math"
	"strings"
	"time"
)

const (
	DefaultOrderCount = 300000
)

var expressStatuses = []string{
	"WAIT",
	"DELIVERING",
	"RECEIVED",
}

var itemTypes = []string{
	"Toys & Games",
	"Automotive",
	"Books",
	"Computers",
	"Luggage",
	"Pet Supplies",
	"Sports & Outdoors",
	"Home & Kitchen",
}

func (w *Workloader) allowAutoRandomExplicitInsert(ctx context.Context) error {
	_, err := w.db.Exec("set @@allow_auto_random_explicit_insert = true;")
	if err != nil {
		return err
	}
	return nil
}

func (w *Workloader) loadOrdersAndExpress(ctx context.Context) error {
	insertOrder := "INSERT INTO orders (id, user_id, amount, item_name, item_type, create_time) VALUES "
	orderLoader := db.NewSQLBatchLoader(w.db, insertOrder, 3, 10)

	insertExpress := "INSERT INTO express (order_id, user_id, post_id, address, current_address, status, create_time) VALUES "
	expressLoader := db.NewSQLBatchLoader(w.db, insertExpress, 3, 10)

	orderSet := make(util.UInt32)
	for w.cfg.OrderCount > 0 && len(orderSet) < w.cfg.OrderCount {
		orderID := uint32(rand.UintRange(1000, math.MaxInt))

		if _, exists := orderSet[orderID]; exists {
			continue
		} else {
			orderSet[orderID] = struct{}{}
		}

		userID := strings.ReplaceAll(rand.UUID(), "-", "")
		amount := rand.Float64Range(10, 10000)
		itemName := rand.Word()
		itemType := rand.RandomString(itemTypes)
		createTimeStart := time.Date(2014, 10, 1, 0, 0, 0, 0, time.UTC)
		createTime := rand.DateRange(createTimeStart, time.Now())

		orderValues := []string{
			fmt.Sprintf(
				`(%d, '%s', %f, '%s', '%s', '%s')`,
				orderID, userID, amount, itemName, itemType, createTime.Format(time.RFC3339),
			),
		}
		if err := orderLoader.InsertValue(ctx, orderValues); err != nil {
			return err
		}

		postID := rand.UUID()
		address := rand.Address().Address
		currentAddress := rand.Address().Address
		status := rand.RandomString(expressStatuses)

		expressValues := []string{
			fmt.Sprintf(
				`(%d, '%s', '%s', '%s', '%s', '%s', '%s')`,
				orderID, userID, postID, address, currentAddress, status, createTime.Format(time.RFC3339),
			),
		}
		if err := expressLoader.InsertValue(ctx, expressValues); err != nil {
			return err
		}
	}

	err := orderLoader.Flush(ctx)
	if err != nil {
		return err
	}
	err = expressLoader.Flush(ctx)
	if err != nil {
		return err
	}
	return nil
}
