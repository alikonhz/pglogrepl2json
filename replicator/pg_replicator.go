package replicator

import (
	"context"
	"errors"
	"fmt"
	"github.com/alikonhz/pglogrepl2json"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"log"
	"time"
)

var (
	errNotSupported   = errors.New("command byte not supported")
	errUnknownMessage = errors.New("unknown message type in PG output stream")
)

// Options provides configuration options for the PgReplicator
type Options struct {
	Conn           *pgconn.PgConn
	SlotName       string
	PubName        string
	StandByTimeout time.Duration
}

type state struct {
	inStream    bool
	pos         pglogrepl.LSN
	nextStandBy time.Time
}

// PgReplicator converts PG logical stream into json
type PgReplicator struct {
	Options

	listener pglogrepl2json.ReplicationListener
	state    *state
}

func NewOptions(conn *pgconn.PgConn, slotName, pubName string, standByTimeout time.Duration) Options {
	var actualTimeout time.Duration
	if standByTimeout > 0 {
		actualTimeout = standByTimeout
	} else {
		actualTimeout = 10 * time.Second // default
	}

	return Options{
		Conn:           conn,
		SlotName:       slotName,
		PubName:        pubName,
		StandByTimeout: actualTimeout,
	}
}

// MustCreate creates new PgReplicator
// panics when listener is nil
func MustCreate(opts Options, listener pglogrepl2json.ReplicationListener) *PgReplicator {
	if listener == nil {
		panic("listener cannot be nil")
	}
	return &PgReplicator{
		Options:  opts,
		listener: listener,
		state:    &state{inStream: false},
	}
}

// StartFromLsn starts reading data from the PG replication connection starting from provided lsn.
// It starts a new goroutine which sends data into the provided Listener
// Cancelling ctx causes PgReplicator to stop sending data.
func (r *PgReplicator) StartFromLsn(ctx context.Context, lsn pglogrepl.LSN) error {
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.Options.PubName),
		"messages 'true'",
		"streaming 'true'",
	}

	err := pglogrepl.StartReplication(ctx,
		r.Options.Conn,
		r.Options.SlotName,
		lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArguments,
		},
	)

	if err != nil {
		return fmt.Errorf("unable to start replication: %w", err)
	}

	go r.read(ctx)

	return nil
}

// Start starts reading data from the PG replication connection from the last saved position of the slot.
// It starts a new goroutine which sends data into the provided Listener
// Cancelling ctx causes PgReplicator to stop sending data.
func (r *PgReplicator) Start(ctx context.Context) error {
	return r.StartFromLsn(ctx, pglogrepl.LSN(0))
}

func (r *PgReplicator) read(ctx context.Context) {
	r.state.nextStandBy = time.Now().Add(r.Options.StandByTimeout)

	for {
		err := r.sendStandBy(ctx)
		if err != nil {
			// TODO
			log.Fatalln("failed to send standby: ", err)
		}

		rawMsg, err := r.receiveMsg(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			// TODO
			log.Fatalln("failed to receive message: ", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			// TODO
			log.Fatalf("received PG wal error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			// TODO
			log.Fatalf("received unexpected message: %T\n", rawMsg)
		}

		err = r.processMsg(msg)
		if err != nil {
			// TODO
			log.Fatalf("failed to process message: %+v\n", err)
		}
	}
}

func (r *PgReplicator) processMsg(msg *pgproto3.CopyData) error {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.processPrimaryKeepAlive(msg.Data[1:])
	case pglogrepl.XLogDataByteID:
		return r.processXLogData(msg.Data[1:])
	}

	return fmt.Errorf("%w: %d", errNotSupported, msg.Data[0])
}

func (r *PgReplicator) processXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse xlogdata: %w", err)
	}
	return r.processV2(xld.WALData)
}

func (r *PgReplicator) processV2(walData []byte) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, r.state.inStream)
	if err != nil {
		return fmt.Errorf("failed to process WAL data: %w", err)
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		return r.listener.OnRelation(logicalMsg)

	case *pglogrepl.BeginMessage:
		return r.listener.OnTxBegin(logicalMsg)

	case *pglogrepl.CommitMessage:
		return r.listener.OnTxCommit(logicalMsg)
	case *pglogrepl.InsertMessageV2:
		return r.listener.OnInsert(logicalMsg)
	case *pglogrepl.UpdateMessageV2:
		return r.listener.OnUpdate(logicalMsg)
	case *pglogrepl.DeleteMessageV2:
		return r.listener.OnDelete(logicalMsg)
	case *pglogrepl.TruncateMessageV2:
		return r.listener.OnTruncate(logicalMsg)
	case *pglogrepl.TypeMessageV2:
		return r.listener.OnType(logicalMsg)
	case *pglogrepl.OriginMessage:
		return r.listener.OnOrigin(logicalMsg)
	case *pglogrepl.LogicalDecodingMessageV2:
		return r.listener.OnLogicalDecodingMessage(logicalMsg)
	case *pglogrepl.StreamStartMessageV2:
		r.state.inStream = true
		return r.listener.OnStreamStart(logicalMsg)
	case *pglogrepl.StreamStopMessageV2:
		r.state.inStream = false
		return r.listener.OnStreamStop(logicalMsg)
	case *pglogrepl.StreamCommitMessageV2:
		return r.listener.OnStreamCommit(logicalMsg)
	case *pglogrepl.StreamAbortMessageV2:
		return r.listener.OnStreamAbort(logicalMsg)
	default:
		return fmt.Errorf("%w: %T", errUnknownMessage, logicalMsg)
	}

	return nil
}

func (r *PgReplicator) processPrimaryKeepAlive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse primary keep alive message: %w", err)
	}
	if pkm.ReplyRequested {
		// reply to server as soon as possible to avoid disconnect because of timeout
		r.state.nextStandBy = time.Time{}
	}
	return nil
}

func (r *PgReplicator) receiveMsg(ctx context.Context) (pgproto3.BackendMessage, error) {
	readCtx, cancel := context.WithDeadline(ctx, r.state.nextStandBy)
	rawMsg, err := r.Options.Conn.ReceiveMessage(readCtx)
	cancel()
	if err != nil {
		return nil, err
	}

	return rawMsg, nil
}

func (r *PgReplicator) sendStandBy(ctx context.Context) error {
	if time.Now().After(r.state.nextStandBy) {
		err := pglogrepl.SendStandbyStatusUpdate(ctx,
			r.Options.Conn,
			pglogrepl.StandbyStatusUpdate{WALWritePosition: r.state.pos},
		)
		if err != nil {
			return err
		}
		r.state.nextStandBy = time.Now().Add(r.Options.StandByTimeout)
	}

	return nil
}
