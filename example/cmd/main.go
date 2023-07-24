package main

import (
	"context"
	"fmt"
	repl2json "github.com/alikonhz/pglogrepl2json/listeners"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	connString, slotName, pubName := readEnv()

	log.Printf("slotName: %q, publication name: %q\n", slotName, pubName)

	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, connString)
	if err != nil {
		log.Fatalln("unable to connect to postgres: ", err)
	}

	defer conn.Close(context.Background())

	opts := replicator.NewOptions(conn, slotName, pubName, 10*time.Second)
	listener := &repl2json.ListenerJSON{}
	repl := replicator.MustCreate(opts, listener)

	err = repl.Start(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	sig := <-c
	fmt.Printf("%+v", sig)
	//time.Sleep(time.Hour)
}

func readEnv() (string, string, string) {
	connString := os.Getenv("PGLOG2JSON_CONN_STRING")
	if connString == "" {
		log.Fatalln("PGLOG2JSON_CONN_STRING is not set")
	}
	slotName := os.Getenv("PGLOG2JSON_SLOT_NAME")
	if slotName == "" {
		slotName = "pglog2json_default_slot"
	}
	pubName := os.Getenv("PGLOG2JSON_PUB_NAME")
	if pubName == "" {
		pubName = slotName + "_pub"
	}
	return connString, slotName, pubName
}
