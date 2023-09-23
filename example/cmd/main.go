package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	repl2json "github.com/alikonhz/pglogrepl2json/listeners"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	integrationtest.MustLoad(".env")

	connString, replConnString, slotName, pubName := readEnv()

	log.Printf("slotName: %q, publication name: %q\n", slotName, pubName)

	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatalln("unable to connect to postgres: ", err)
	}
	err = createTableSlotAndPub(pool, slotName, pubName)
	defer pool.Close()

	replConn, err := pgconn.Connect(context.Background(), replConnString)
	if err != nil {
		log.Fatalln("unable to connect to postgres via replication connection: ", err)
	}

	defer replConn.Close(context.Background())

	opts := replicator.NewOptions(replConn, slotName, pubName, 10*time.Second)
	outCh := make(chan []byte)
	listener := repl2json.MustCreateNewJson(outCh, repl2json.ListenerJSONOptions{})
	repl := replicator.MustCreate(opts, listener)
	doneChan := make(chan struct{})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	go writeToTable(ctx, pool)
	go printJson(ctx, outCh, doneChan)
	defer cancel()
	err = repl.Start(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	<-ctx.Done()
	<-doneChan
}

func writeToTable(ctx context.Context, pool *pgxpool.Pool) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			insert(ctx, pool)
		}
	}
}

func insert(ctx context.Context, pool *pgxpool.Pool) {
	data := make([]byte, 1024)
	rand.Read(data)
	text := base64.StdEncoding.EncodeToString(data)
	_, err := pool.Exec(ctx, "insert into test_pub_table (data) values ($1)", text)
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}

func createTableSlotAndPub(conn *pgxpool.Pool, slotName string, pubName string) error {
	_, err := conn.Exec(context.Background(), "CREATE TABLE test_pub_table (id bigserial, data text)")
	if err != nil {
		return fmt.Errorf("unable to create table: %w", err)
	}
	_, err = conn.Exec(context.Background(), "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName)
	if err != nil {
		return fmt.Errorf("unable to create slot: %w", err)
	}
	_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE test_pub_table", pubName))
	if err != nil {
		return fmt.Errorf("unable to create publication: %w", err)
	}
	return nil
}

func printJson(ctx context.Context, ch <-chan []byte, doneChan chan struct{}) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case bytes := <-ch:
			fmt.Println(string(bytes))
		}
	}

	doneChan <- struct{}{}
}

func readEnv() (connString, replConnString, slotName, pubName string) {
	connString = os.Getenv("PGLOG2JSON_CONN_STRING")
	if connString == "" {
		log.Fatalln("PGLOG2JSON_CONN_STRING is not set")
	}
	const replSuffix = "?replication=database"
	if strings.HasSuffix(connString, replSuffix) {
		replConnString = connString
		connString = strings.TrimSuffix(connString, replSuffix)
	} else {
		replConnString = connString + replSuffix
	}
	slotName = os.Getenv("PGLOG2JSON_SLOT_NAME")
	if slotName == "" {
		slotName = "pglog2json_default_slot"
	}
	pubName = os.Getenv("PGLOG2JSON_PUB_NAME")
	if pubName == "" {
		pubName = slotName + "_pub"
	}
	return connString, replConnString, slotName, pubName
}
