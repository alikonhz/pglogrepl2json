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

const (
	commitTxMsg = "could not get commit timestamp data. Make sure the configuration parameter \"track_commit_timestamp\" is set"
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

	// JSON channel, we're expecting 3 entries
	c := make(chan string, 3)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertSimpleData)

	assert.Equal(t, 3, len(receivedJSON))
	assertBegin(t, xid, commitTime, receivedJSON[0])
	assertSimpleInsert(t, tOpts.table, receivedJSON[1])
	assertCommit(t, receivedJSON[2])
}

func TestTxWithUpdate(t *testing.T) {
	c := make(chan string, 4)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertUpdateSimpleData)

	assert.Equal(t, 4, len(receivedJSON))
	assertBegin(t, xid, commitTime, receivedJSON[0])
	assertSimpleInsert(t, tOpts.table, receivedJSON[1])
	assertSimpleUpdate(t, tOpts.table, receivedJSON[2])
	assertCommit(t, receivedJSON[3])
}

func runTest(t *testing.T, c chan string, test func(ctx context.Context, tOpts testData) uint32) (testData, uint32, time.Time, []string) {

	ctx, cancel := context.WithCancel(context.Background())
	tOpts := mustCreateTestOptions(ctx)
	defer tOpts.conn.Close(ctx)
	defer cancel()

	jsonListener := listeners.MustCreateNewJson(c, listeners.ListenerJSONOptions{IncludeTimestamp: true})

	repl := MustCreate(tOpts.createReplOptions(), jsonListener)

	err := repl.Start(ctx)
	assert.NoError(t, err)

	xid := test(ctx, tOpts)
	commitTime, err := readCommitTime(ctx, xid)
	assert.NoError(t, err, commitTxMsg)
	assert.NotEqual(t, 0, xid)

	receivedJSON := waitForTestDataWithin(time.Second, c)

	return tOpts, xid, commitTime, receivedJSON
}

func readCommitTime(ctx context.Context, xid uint32) (time.Time, error) {
	conn := integrationtest.MustCreateDbConnection()
	defer conn.Close()

	res := conn.QueryRow(ctx, "select pg_xact_commit_timestamp($1)", xid)
	var t time.Time
	err := res.Scan(&t)
	return t, err
}

func assertSimpleInsert(t *testing.T, table string, insertJSON string) {
	m := mustParseToMap(insertJSON)
	assert.Equal(t, "I", m[listeners.ActionKey])
	assert.Equal(t, "public", m[listeners.SchemaKey])
	assert.Equal(t, table, m[listeners.TableKey])
	valMap, err := readMapFromKey(t, m, listeners.ColumnsKey)
	assert.NoError(t, err)
	assertValInMap(t, valMap, "id", 1)
	assertValInMap(t, valMap, "field_int", 1)
}

func assertSimpleUpdate(t *testing.T, table string, updateJSON string) {
	m := mustParseToMap(updateJSON)
	assert.Equal(t, "U", m[listeners.ActionKey])
	assert.Equal(t, "public", m[listeners.SchemaKey])
	assert.Equal(t, table, m[listeners.TableKey])

	newValMap, err := readMapFromKey(t, m, listeners.ColumnsKey)
	assert.NoError(t, err)
	assertValInMap(t, newValMap, "id", 1)
	assertValInMap(t, newValMap, "field_int", 2)

	oldValMap, err := readMapFromKey(t, m, listeners.IdentityKey)
	assert.NoError(t, err)
	assertValInMap(t, oldValMap, "id", 1)
	assertValInMap(t, oldValMap, "field_int", 1)
}

func readMapFromKey(t *testing.T, m map[string]any, key string) (map[string]any, error) {
	valMapAny, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("%q is not found", key)
	}
	valMap, ok := valMapAny.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%q is not a map[string]any", key)
	}

	return valMap, nil
}

func assertTimestamp(t *testing.T, m map[string]any, key, format string, expectedTime time.Time) {
	ts, ok := m[key]
	assert.True(t, ok, fmt.Sprintf("key %q is not found", key))
	tsStr, ok := ts.(string)
	assert.True(t, ok, fmt.Sprintf("key %q is not a string", key))
	timeJSON, err := time.Parse(format, tsStr)
	assert.NoError(t, err, "invalid format")
	assert.Equal(t, expectedTime, timeJSON)
}

func assertCommit(t *testing.T, commitJson string) {
	m := mustParseToMap(commitJson)
	assert.Equal(t, "C", m[listeners.ActionKey])
}

func assertBegin(t *testing.T, xid uint32, commitTime time.Time, beginJson string) {
	assert.Equal(t, false, commitTime.IsZero(), commitTxMsg)
	m := mustParseToMap(beginJson)

	assert.Equal(t, "B", m[listeners.ActionKey])
	assertValInMap(t, m, listeners.XidKey, xid)
	assertTimestamp(t, m, listeners.TimestampKey, listeners.TimestampFormat, commitTime)
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

func insertUpdateSimpleData(ctx context.Context, tOpts testData) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		_, err = tx.Exec(ctx, fmt.Sprintf("update %s set field_int = 2 where id = 1", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to update simple data: %w", err)
		}

		return nil
	})
}

func insertSimpleData(ctx context.Context, tOpts testData) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		return nil
	})
}

func mustCreateTestOptions(ctx context.Context) testData {
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
