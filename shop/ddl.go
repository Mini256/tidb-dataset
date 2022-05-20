package shop

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

const (
	tableOrders   = "orders"
	tableExpress  = "express"
	tableOrderAgg = "orders_agg"
)

var tableNames = []string{
	tableOrders, tableExpress, tableOrderAgg,
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
	s := getShopState(ctx)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

// createTables creates tables schema.
func (w *ddlManager) createTables(ctx context.Context) error {
	// Orders.
	query := `
		CREATE TABLE IF NOT EXISTS orders (
			id bigint(20) primary key auto_random,
			user_id varchar(32) comment '用户ID',
			amount decimal(20,2) comment '订单金额',
			item_name varchar(128) comment '商品名称',
			item_type varchar(32) comment '商品类型',
			create_time datetime default now() comment '订单创建时间',
			index idx_create_time_user_id (create_time,user_id)
		)
	`
	w.log.Printf("Creating table %s.\n", tableOrders)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Express.
	query = `
		CREATE TABLE IF NOT EXISTS express (
			id bigint(20) primary key auto_random,
			order_id bigint(20) comment '订单编号',
			user_id varchar(32) comment '用户ID',
			post_id varchar(64) comment '快递单号',
			address varchar(256) comment '收货地址',
			current_address varchar(256) comment '当前配送地址',
			status varchar(10) comment '快件派送状态',
			create_time datetime default now() comment '快递创建时间',
			index idx_order_id (order_id)
		)
	`
	w.log.Printf("Creating table %s.\n", tableExpress)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	// Express.
	query = `
		CREATE TABLE IF NOT EXISTS orders_agg (
			user_id varchar(32) comment '用户ID',
			create_month varchar(32) comment '月份',
			total_amount decimal(20,2) comment '当月总支出金额',
			primary key (user_id, create_month)
		)
	`
	w.log.Printf("Creating table %s.\n", tableOrderAgg)
	if err := w.execTableDDL(ctx, query); err != nil {
		return err
	}

	w.log.Info("Finished creating tables!")

	return nil
}

// dropTables creates tables schema.
func (w *ddlManager) dropTables(ctx context.Context) error {
	dropTables := []string{
		tableOrders, tableExpress, tableOrderAgg,
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
