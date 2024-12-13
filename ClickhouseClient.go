package main

import (
	"context"
	"crypto/tls"
	"github.com/ClickHouse/clickhouse-go/v2"
	json2 "github.com/goccy/go-json"
	"github.com/google/uuid"
	"time"
)

func handleInsert(timestamp string, author string, content string, uri string, cid string) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{"e52uq9yb5t.us-east-1.aws.clickhouse.cloud:9440"},
		Protocol: clickhouse.Native,
		TLS: &tls.Config{
			InsecureSkipVerify: true, // Only for testing
		},
		Auth: clickhouse.Auth{
			Username: "default",
			Password: "LD9tScvlDiFy.",
		},
		Settings: clickhouse.Settings{
			"max_execution_time":    60,
			"async_insert":          1,
			"wait_for_async_insert": 1,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Duration(10) * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		BlockBufferSize:  10,
	})

	if err != nil {

		return err
	}
	ctx := context.Background()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO activity")
	if err != nil {
		return err
	}
	for i := 0; i < 1000; i++ {
		err := batch.Append(
			uuid.New(),
			timestamp,
			author,
			content,
			uri,
			cid,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()

}

func handleSavePost(dbPassword string, timestamp string, did string, kind string, collection string, operation string, cid string, record json2.RawMessage) interface{} {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{"e52uq9yb5t.us-east-1.aws.clickhouse.cloud:9440"},
		Protocol: clickhouse.Native,
		TLS: &tls.Config{
			InsecureSkipVerify: true, // Only for testing
		},
		Auth: clickhouse.Auth{
			Username: "default",
			Password: dbPassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":    60,
			"async_insert":          1,
			"wait_for_async_insert": 1,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Duration(10) * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		BlockBufferSize:  10,
	})

	if err != nil {

		return err
	}
	ctx := context.Background()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO posts")
	if err != nil {
		return err
	}
	err = batch.Append(
		uuid.New(),
		timestamp,
		did,
		kind,
		collection,
		operation,
		cid,
		string(record),
	)
	if err != nil {
		return err
	}
	return batch.Send()

}
