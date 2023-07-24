package replicator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/listeners"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var (
	errTestTimeout = errors.New("test didn't finish in time")
)

type testData struct {
	table   string
	slot    string
	pub     string
	conn    *pgconn.PgConn
	timeout time.Duration
}

func (t testData) createReplOptions() Options {
	return NewOptions(t.conn, t.slot, t.pub, t.timeout)
}

func TestSimpleTx(t *testing.T) {
	tOpts := mustCreateTestOptions()
	ctx, cancel := context.WithCancel(context.Background())

	defer tOpts.conn.Close(ctx)

	// JSON channel
	c := make(chan string, 3)
	jsonListener := listeners.MustCreateNewJson(c)

	repl := MustCreate(tOpts.createReplOptions(), jsonListener)
	err := repl.Start(ctx)
	assert.NoError(t, err)
	xid := insertSimpleData(ctx, tOpts.table)
	assert.NotEqual(t, 0, xid)
	receivedJSON := waitForTestDataWithin(time.Second, c)
	cancel()
	close(c)

	assert.Equal(t, 3, len(receivedJSON))
	assertBegin(t, receivedJSON[0], xid)
	assertSimpleInsert(t, tOpts.table, receivedJSON[1])
	assertCommit(t, receivedJSON[2])
}

func assertSimpleInsert(t *testing.T, table string, insertJson string) {
	m := mustParseToMap(insertJson)
	assert.Equal(t, "public", m["schema"])
	assert.Equal(t, table, m["table"])
	valMapAny, ok := m["new"]
	assert.True(t, ok, "new values were not found")
	valMap, ok := valMapAny.(map[string]any)
	assert.True(t, ok, "new is not map[string]any")
	assertValInMap(t, valMap, "field_int", 1)
}

func assertCommit(t *testing.T, commitJson string) {
	m := mustParseToMap(commitJson)
	assert.Equal(t, "C", m["action"])
}

func assertBegin(t *testing.T, beginJson string, xid uint32) {
	m := mustParseToMap(beginJson)
	assert.Equal(t, "B", m["action"])
	assertValInMap(t, m, "xid", xid)
}

func assertValInMap[T int | uint32 | int64](t *testing.T, m map[string]any, key string, val T) {
	f, ok := m[key].(float64)
	assert.True(t, ok, fmt.Sprintf("%s is not a float64", key))
	i := T(f)
	assert.Equal(t, val, i)
}

func mustParseToMap(j string) map[string]any {
	m := make(map[string]any)
	err := json.Unmarshal([]byte(j), &m)
	if err != nil {
		panic(err)
	}

	return m
}

func waitForTestDataWithin(t time.Duration, c chan string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	receivedJSON := make([]string, 0)
f:
	for {
		select {
		case <-ctx.Done():
			return receivedJSON
		case j, ok := <-c:
			if !ok {
				break f
			}
			receivedJSON = append(receivedJSON, j)
		}
	}

	return receivedJSON
}

func insertSimpleData(ctx context.Context, tableName string) uint32 {
	conn := integrationtest.MustCreateDbConnection()
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		panic(err)
	}

	defer tx.Commit(ctx)

	_, err = tx.Exec(ctx, fmt.Sprintf("insert into %s (field_int) values (1)", tableName))

	if err != nil {
		panic("unable to insert simple data")
	}

	xid := mustReadXid(ctx, tx)

	return xid
}

func mustReadXid(ctx context.Context, tx pgx.Tx) uint32 {
	r, err := tx.Query(ctx, "select txid_current()")
	if err != nil {
		panic(fmt.Errorf("unable to get current xid: %v", err))
	}

	var xid uint32
	for r.Next() {
		err = r.Scan(&xid)
		if err != nil {
			panic(fmt.Errorf("unable to read xid from result: %v", err))
		}
	}

	return xid
}

func mustCreateTestOptions() testData {
	ctx := context.Background()
	table, slot, pub := integrationtest.MustCreateTableSlotAndPub(ctx)
	pgConn := integrationtest.MustCreateReplConnection()
	timeout := 10 * time.Second

	return testData{
		table:   table,
		slot:    slot,
		pub:     pub,
		conn:    pgConn,
		timeout: timeout,
	}
}

func TestMain(m *testing.M) {
	integrationtest.MustLoad("../.env-test")
	integrationtest.Cleanup()
	os.Exit(m.Run())
}
