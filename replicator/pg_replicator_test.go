package replicator

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/listeners"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
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

type streamData struct {
	insertValue []byte
	updateValue []byte
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

func TestTxWithDelete(t *testing.T) {
	// begin, insert, commit -> first tx
	// begin, delete, commit -> second tx
	c := make(chan string, 6)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertDeleteSimpleData)
	assert.Equal(t, 6, len(receivedJSON))

	assertBegin(t, xid, commitTime, receivedJSON[3])
	assertSimpleDelete(t, tOpts.table, receivedJSON[4])
	assertCommit(t, receivedJSON[5])
}

func TestTxWithTruncate(t *testing.T) {
	c := make(chan string, 6)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertTruncateSimpleData)

	assert.Equal(t, 6, len(receivedJSON))

	assertBegin(t, xid, commitTime, receivedJSON[3])
	assertTruncate(t, tOpts.table, receivedJSON[4])
	assertCommit(t, receivedJSON[5])
}

func TestTxWithLogicalTransactionalMessage(t *testing.T) {

	t.Run("logical decoding msg hex (transaction)", func(t *testing.T) {
		testTxWithLogicalTranMsg(t, listeners.BinaryEncodingHex)
	})

	t.Run("logical decoding msg base64 (transaction)", func(t *testing.T) {
		testTxWithLogicalTranMsg(t, listeners.BinaryEncodingBase64)
	})
}

func TestTxStream(t *testing.T) {
	// streaming of in-progress transactions is supported since PG version 14
	// streaming is controlled by the "logical_decoding_work_mem" PG config parameter
	// for this test to work it should be set to minimum value (64Kb)
	// call to integrationtest.MustLogicalDecodingWorkMem() makes sure that it's the case
	integrationtest.MustLogicalDecodingWorkMem()

	// stream start, insert, stream stop, stream start, update, stream stop, stream commit
	c := make(chan string, 7)
	xData := &streamData{}
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, func(ctx context.Context, tOpts testData) uint32 {
		return testStream(ctx, tOpts, xData)
	})

	assert.Equal(t, 7, len(receivedJSON))
	assertStreamStart(t, xid, true, receivedJSON[0])
	assertInsert(t, tOpts.table, xData.insertValue, receivedJSON[1])
	assertStreamStop(t, receivedJSON[2])
	assertStreamStart(t, xid, false, receivedJSON[3])
	assertUpdate(t, tOpts.table, xData.insertValue, xData.updateValue, receivedJSON[4])
	assertStreamStop(t, receivedJSON[5])
	assertStreamCommit(t, xid, commitTime, receivedJSON[6])
}

func assertStreamCommit(t *testing.T, xid uint32, commitTime time.Time, commitJSON string) {
	m := mustParseToMap(commitJSON)
	assert.Equal(t, listeners.StreamCommit, m[listeners.ActionKey])
	assertValInMap(t, m, listeners.XidKey, xid)
	assertTimestamp(t, m, listeners.TimestampKey, listeners.TimestampFormat, commitTime)
}

func assertUpdate(t *testing.T, table string, oldData []byte, newData []byte, updateJSON string) {
	oldValMap, newValMap := assertSimpleUpdate(t, table, updateJSON)
	assertSliceOfBytes(t, oldValMap, "field_data", oldData)
	assertSliceOfBytes(t, newValMap, "field_data", newData)
}

func assertInsert(t *testing.T, table string, data []byte, insertJSON string) {
	valMap := assertSimpleInsert(t, table, insertJSON)
	assertSliceOfBytes(t, valMap, "field_data", data)
}

func assertSliceOfBytes(t *testing.T, m map[string]any, key string, data []byte) {
	valStr, ok := m[key].(string)
	assert.True(t, ok, fmt.Sprintf("field_data is not string"))
	valData, err := base64.StdEncoding.DecodeString(valStr)
	assert.NoError(t, err, "field_data is not valid base64 string")
	assert.Equal(t, data, valData)
}

func assertStreamStart(t *testing.T, xid uint32, firstSegment bool, startJSON string) {
	m := mustParseToMap(startJSON)
	assert.Equal(t, listeners.StreamStart, m[listeners.ActionKey])
	assertValInMap(t, m, listeners.XidKey, xid)
	assert.Equal(t, firstSegment, m[listeners.FirstSegmentKey])
}

func assertStreamStop(t *testing.T, stopJSON string) {
	m := mustParseToMap(stopJSON)
	assert.Equal(t, listeners.StreamStop, m[listeners.ActionKey])
}

func testStream(ctx context.Context, tOpts testData, xData *streamData) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		const kb100 = 100 * 1024
		data := generateRandomData(kb100)
		xData.insertValue = data

		_, err := tx.Exec(ctx,
			fmt.Sprintf("insert into %s (id, field_int, field_data) values (1, 1, $1)", tOpts.table),
			data,
		)
		if err != nil {
			return fmt.Errorf("unable to insert data: %w", err)
		}

		data = generateRandomData(kb100)
		xData.updateValue = data

		_, err = tx.Exec(ctx,
			fmt.Sprintf("update %s set field_data = $1, field_int = 2 where id = 1", tOpts.table),
			data,
		)
		if err != nil {
			return fmt.Errorf("unable to update data: %w", err)
		}

		return nil
	})
}

func generateRandomData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func testTxWithLogicalTranMsg(t *testing.T, msgContentEnc byte) {

	c := make(chan string, 4)
	tOpts, xid, commitTime, receivedJSON := runTestWithOpts(t,
		c,
		insertWithTransactionalMessage,
		listeners.ListenerJSONOptions{BinaryContentFormat: msgContentEnc},
	)

	assert.Equal(t, 4, len(receivedJSON))
	assertBegin(t, xid, commitTime, receivedJSON[0])
	assertSimpleInsert(t, tOpts.table, receivedJSON[1])
	assertMsg(t, receivedJSON[2], true, msgContentEnc)
	assertCommit(t, receivedJSON[3])
}

func TestTxWithLogicalMessage(t *testing.T) {
	t.Run("logical decoding msg hex", func(t *testing.T) {
		testTxWithLogicalMsg(t, listeners.BinaryEncodingHex)
	})

	t.Run("logical decoding msg base64", func(t *testing.T) {
		testTxWithLogicalMsg(t, listeners.BinaryEncodingBase64)
	})
}

func testTxWithLogicalMsg(t *testing.T, msgContentEnc byte) {
	c := make(chan string, 4)
	tOpts, xid, commitTime, receivedJSON := runTestWithOpts(t,
		c,
		insertWithMessage,
		listeners.ListenerJSONOptions{BinaryContentFormat: msgContentEnc},
	)

	assert.Equal(t, 4, len(receivedJSON))
	assertMsg(t, receivedJSON[0], false, msgContentEnc)
	assertBegin(t, xid, commitTime, receivedJSON[1])
	assertSimpleInsert(t, tOpts.table, receivedJSON[2])
	assertCommit(t, receivedJSON[3])
}

func runTestWithOpts(t *testing.T,
	c chan string,
	test func(ctx context.Context, tOpts testData) uint32,
	opts listeners.ListenerJSONOptions) (testData, uint32, time.Time, []string) {

	ctx, cancel := context.WithCancel(context.Background())
	tOpts := mustCreateTestOptions(ctx)
	defer tOpts.conn.Close(ctx)
	defer cancel()

	jsonListener := listeners.MustCreateNewJson(c, opts)

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

func runTest(t *testing.T, c chan string, test func(ctx context.Context, tOpts testData) uint32) (testData, uint32, time.Time, []string) {
	return runTestWithOpts(t, c, test, listeners.ListenerJSONOptions{})
}

func readCommitTime(ctx context.Context, xid uint32) (time.Time, error) {
	conn := integrationtest.MustCreateDbConnection()
	defer conn.Close()

	res := conn.QueryRow(ctx, "select pg_xact_commit_timestamp($1)", xid)
	var t time.Time
	err := res.Scan(&t)
	return t, err
}

func assertMsg(t *testing.T, msgJSON string, transactional bool, msgContentEnc byte) {
	m := mustParseToMap(msgJSON)
	assert.Equal(t, "M", m[listeners.ActionKey])
	assert.Equal(t, transactional, m[listeners.TransactionalKey])
	assert.Equal(t, "test", m[listeners.PrefixKey])

	contentAny := m[listeners.ContentKey]
	assert.NotNil(t, contentAny, fmt.Sprintf("%q is nill", listeners.ContentKey))
	content, ok := contentAny.(string)
	assert.True(t, ok, fmt.Sprintf("%q is not string", listeners.ContentKey))

	var decodedContent string
	switch msgContentEnc {
	case listeners.BinaryEncodingHex:
		decodedContent = mustDecodeFromHex(content)
	case listeners.BinaryEncodingBase64:
		decodedContent = mustDecodeFromBase64(content)
	default:
		panic(fmt.Errorf("unsupported logical message content encoding: %d", msgContentEnc))
	}

	assert.Equal(t, "test_message", decodedContent)
}

func mustDecodeFromHex(content string) string {
	data, err := hex.DecodeString(content)
	if err != nil {
		panic(err)
	}

	return string(data)
}

func mustDecodeFromBase64(content string) string {
	data, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		panic(err)
	}

	return string(data)
}

func assertSimpleInsert(t *testing.T, table string, insertJSON string) map[string]any {
	m := mustParseToMap(insertJSON)
	assert.Equal(t, listeners.Insert, m[listeners.ActionKey])
	assert.Equal(t, "public", m[listeners.SchemaKey])
	assert.Equal(t, table, m[listeners.TableKey])
	valMap, err := readMapFromKey(t, m, listeners.ColumnsKey)
	assert.NoError(t, err)
	assertValInMap(t, valMap, "id", 1)
	assertValInMap(t, valMap, "field_int", 1)

	return valMap
}

func assertSimpleUpdate(t *testing.T, table string, updateJSON string) (oldValMap map[string]any, newValMap map[string]any) {
	m := mustParseToMap(updateJSON)
	assert.Equal(t, listeners.Update, m[listeners.ActionKey])
	assert.Equal(t, "public", m[listeners.SchemaKey])
	assert.Equal(t, table, m[listeners.TableKey])

	var err error

	newValMap, err = readMapFromKey(t, m, listeners.ColumnsKey)
	assert.NoError(t, err)
	assertValInMap(t, newValMap, "id", 1)
	assertValInMap(t, newValMap, "field_int", 2)

	oldValMap, err = readMapFromKey(t, m, listeners.IdentityKey)
	assert.NoError(t, err)
	assertValInMap(t, oldValMap, "id", 1)
	assertValInMap(t, oldValMap, "field_int", 1)

	return oldValMap, newValMap
}

func assertTruncate(t *testing.T, table string, truncateJSON string) {
	m := mustParseToMap(truncateJSON)
	assert.Equal(t, listeners.Truncate, m[listeners.ActionKey])
	assert.Equal(t, "public", m[listeners.SchemaKey])
	assert.Equal(t, table, m[listeners.TableKey])
}

func assertSimpleDelete(t *testing.T, table string, deleteJSON string) {
	m := mustParseToMap(deleteJSON)
	assert.Equal(t, listeners.Delete, m[listeners.ActionKey])
	assert.Equal(t, "public", m[listeners.SchemaKey])
	assert.Equal(t, table, m[listeners.TableKey])

	// in delete there's only "identity" (i.e. previous values)
	assert.Nil(t, m[listeners.ColumnsKey])
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
	assert.Equal(t, listeners.Commit, m[listeners.ActionKey])
}

func assertBegin(t *testing.T, xid uint32, commitTime time.Time, beginJson string) {
	assert.Equal(t, false, commitTime.IsZero(), commitTxMsg)
	m := mustParseToMap(beginJson)

	assert.Equal(t, listeners.Begin, m[listeners.ActionKey])
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

func insertTruncateSimpleData(ctx context.Context, tOpts testData) uint32 {
	insertSimpleData(ctx, tOpts)

	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("truncate table %s", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to truncate table: %w", err)
		}

		return nil
	})
}

func insertDeleteSimpleData(ctx context.Context, tOpts testData) uint32 {
	insertSimpleData(ctx, tOpts)

	// we need xid only of the last transaction
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("delete from %s where id = 1", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to delete simple data: %w", err)
		}

		return nil
	})
}

func insertWithTransactionalMessage(ctx context.Context, tOpts testData) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		_, err = tx.Exec(ctx, "select pg_logical_emit_message(true, 'test', 'test_message')")
		if err != nil {
			return fmt.Errorf("unable to send transactional message: %w", err)
		}

		return nil
	})
}

func insertWithMessage(ctx context.Context, tOpts testData) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		_, err = tx.Exec(ctx, "select pg_logical_emit_message(false, 'test', 'test_message')")
		if err != nil {
			return fmt.Errorf("unable to send transactional message: %w", err)
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
