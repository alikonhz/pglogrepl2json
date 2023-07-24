package integrationtest

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"math/rand"
	"os"
	"strings"
)

const (
	EnvPgTestConn = "PG_TEST_CONN"
)

type table string
type slot string
type pub string

func MustLoad(fileName string) {
	fmt.Println("reading ", fileName)
	err := godotenv.Load(fileName)
	if err != nil {
		panic(err)
	}
}

func MustCreateDbConnection() *pgxpool.Pool {
	pgConnStr := os.Getenv(EnvPgTestConn)
	if pgConnStr == "" {
		panic("PG_TEST_CONN not set")
	}

	conn, err := pgxpool.New(context.Background(), pgConnStr)
	if err != nil {
		panic(err)
	}

	return conn
}

func MustCreateTableSlotAndPub(ctx context.Context) (string, string, string) {
	b := make([]byte, 4)
	rand.Read(b)
	s := hex.EncodeToString(b)

	conn := MustCreateDbConnection()
	tableName := fmt.Sprintf("json_test_%s", s)
	slotName := fmt.Sprintf("%s_slot", tableName)
	pubName := fmt.Sprintf("%s_pub", tableName)

	err := createTable(ctx, conn, table(tableName))
	if err != nil {
		panic(err)
	}

	err = createSlotAndPub(ctx, conn, table(tableName), slot(slotName), pub(pubName))
	if err != nil {
		panic(err)
	}

	return tableName, slotName, pubName
}

func Cleanup() {
	conn := MustCreateDbConnection()
	jsonTestTables, err := readJsonTestTables(conn)

	if err != nil {
		panic(fmt.Errorf("unable to cleanup test database: %v", err))
	}

	for _, table := range jsonTestTables {
		sName, pName := slotAndPubName(table)
		err := DropSlotAndPub(context.Background(), conn, sName, pName)
		if err != nil {
			panic(fmt.Errorf("unable to drop slot %s or pub %s: %w", sName, pName, err))
		}
		err = DropTable(context.Background(), conn, table)
		if err != nil {
			panic(fmt.Errorf("unable to drop table %s: %w", table, err))
		}
	}
}

func DropTable(ctx context.Context, conn *pgxpool.Pool, tName table) error {
	dropTable := fmt.Sprintf("DROP TABLE IF EXISTS %s", tName)
	_, err := conn.Exec(ctx, dropTable)
	return err
}

func DropSlotAndPub(ctx context.Context, conn *pgxpool.Pool, sName slot, pName pub) error {
	dropPub := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pName)
	_, err := conn.Exec(ctx, dropPub)
	if err != nil {
		return err
	}
	dropSlot := `SELECT pg_drop_replication_slot($1)
				  WHERE EXISTS (SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1)`
	_, err = conn.Exec(ctx, dropSlot, sName)

	return err
}

func createSlotAndPub(ctx context.Context, conn *pgxpool.Pool, tName table, sName slot, pName pub) error {
	fmt.Printf("creating replication slot %s\n", sName)

	slotQuery := "SELECT pg_create_logical_replication_slot($1, 'pgoutput')"
	_, err := conn.Exec(ctx, slotQuery, sName)
	if err != nil {
		return err
	}
	pubQuery := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pName, tName)
	_, err = conn.Exec(ctx, pubQuery)
	return err
}

func createTable(ctx context.Context, conn *pgxpool.Pool, tName table) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
			  (
			    id BIGSERIAL PRIMARY KEY,
			    field_int INTEGER
			  )`, tName)
	_, err := conn.Exec(ctx, query)
	return err
}

func slotAndPubName(tName table) (slot, pub) {
	return slot(fmt.Sprintf("%s_slot", tName)), pub(fmt.Sprintf("%s_pub", tName))
}

func readJsonTestTables(conn *pgxpool.Pool) ([]table, error) {
	tablesQuery := "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'json_test%'"
	res, err := conn.Query(context.Background(), tablesQuery)
	if err != nil {
		return nil, fmt.Errorf("read tables: %w", err)
	}
	defer res.Close()

	var tables []table
	for res.Next() {
		var tName table
		err = res.Scan(&tName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, tName)
	}

	return tables, nil
}

func MustCreateReplConnection() *pgconn.PgConn {
	pgConnStr := os.Getenv(EnvPgTestConn)
	if pgConnStr == "" {
		panic("PG_TEST_CONN not set")
	}

	if !strings.HasSuffix(pgConnStr, "?replication=database") {
		pgConnStr += "?replication=database"
	}

	conn, err := pgconn.Connect(context.Background(), pgConnStr)
	if err != nil {
		panic(err)
	}

	return conn
}
