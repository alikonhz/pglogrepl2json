package listeners

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"time"
)

var (
	errRelationNotFound = "relation not found"
)

const (
	pgTypeBytea uint32 = 17
)

const (
	Begin        = "B"
	Insert       = "I"
	Update       = "U"
	Commit       = "c"
	Delete       = "D"
	Message      = "M"
	Truncate     = "T"
	StreamStart  = "AB"
	StreamStop   = "AS"
	StreamCommit = "AC"
	StreamAbort  = "AA"
)

const (
	XidKey           = "xid"
	SubXidKey        = "sub_xid"
	ActionKey        = "action"
	SchemaKey        = "schema"
	TableKey         = "table"
	ColumnsKey       = "columns"
	IdentityKey      = "identity"
	TimestampKey     = "timestamp"
	TransactionalKey = "transactional"
	ContentKey       = "content"
	PrefixKey        = "prefix"
	FirstSegmentKey  = "first_segment"

	TimestampFormat = time.RFC3339Nano
)

var (
	commit     = []byte(`{ "action": "` + Commit + `" }`)
	streamStop = []byte(`{ "action": "` + StreamStop + `" }`)
)

const (
	// BinaryEncodingBase64 specifies that all []byte data should be encoded as base64 string. This is the default value
	BinaryEncodingBase64 byte = 0
	// BinaryEncodingHex specifies that all []byte data should be encoded as HEX string
	BinaryEncodingHex byte = 1
)

// ListenerJSONOptions contains options for JSON listener
type ListenerJSONOptions struct {
	// BinaryContentFormat controls how []byte data will be encoded. It can be either BinaryEncodingBase64 or BinaryEncodingHex.
	// If other value is set then BinaryEncodingBase64 is used
	BinaryContentFormat byte
}

// ListenerJSON transforms output from PG replication into JSON format similar to wal2json plugin
type ListenerJSON struct {
	ListenerJSONOptions
	c         chan []byte
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
}

// MustCreateNewJson creates a new JSON listener
// it panics if c is nil
func MustCreateNewJson(c chan []byte, opts ListenerJSONOptions) *ListenerJSON {
	if c == nil {
		panic("channel is nil")
	}
	return &ListenerJSON{
		ListenerJSONOptions: opts,
		c:                   c,
		relations:           make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:             pgtype.NewMap(),
	}
}

// OnOrigin is called when OriginMessage is received from replication connection
func (s *ListenerJSON) OnOrigin(msg *pglogrepl.OriginMessage) error {
	// not supported
	return nil
}

// OnTxBegin is called when BeginMessage is received from replication connection
func (s *ListenerJSON) OnTxBegin(msg *pglogrepl.BeginMessage) error {
	m := make(map[string]any)
	m[ActionKey] = Begin
	m[XidKey] = msg.Xid

	m[TimestampKey] = msg.CommitTime.Format(TimestampFormat)

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnTxCommit is called when CommitMessage is received from replication connection
func (s *ListenerJSON) OnTxCommit(msg *pglogrepl.CommitMessage) error {

	s.c <- commit

	return nil
}

// OnStreamStart is called when StreamStartMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = StreamStart
	m[XidKey] = msg.Xid

	const firstSegment uint8 = 1
	m[FirstSegmentKey] = msg.FirstSegment == firstSegment

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnStreamStop is called when StreamStopMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamStop(_ *pglogrepl.StreamStopMessageV2) error {
	s.c <- streamStop
	return nil
}

// OnStreamCommit is called when StreamCommitMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = StreamCommit
	m[XidKey] = msg.Xid
	m[TimestampKey] = msg.CommitTime.Format(TimestampFormat)

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnStreamAbort is called when StreamAbortMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = StreamAbort
	m[XidKey] = msg.Xid
	m[SubXidKey] = msg.SubXid

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnInsert is called when InsertMessageV2 is received from replication connection
func (s *ListenerJSON) OnInsert(msg *pglogrepl.InsertMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Insert
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}
	m[SchemaKey] = rel.Namespace
	m[TableKey] = rel.RelationName
	v, err := s.readTuple(msg.Tuple, rel.Columns)
	if err != nil {
		return err
	}

	m[ColumnsKey] = v

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnUpdate is called when UpdateMessageV2 is received from replication connection
func (s *ListenerJSON) OnUpdate(msg *pglogrepl.UpdateMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Update
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}

	m[SchemaKey] = rel.Namespace
	m[TableKey] = rel.RelationName

	n, err := s.readTuple(msg.NewTuple, rel.Columns)
	if err != nil {
		return err
	}
	m[ColumnsKey] = n

	o, err := s.readTuple(msg.OldTuple, rel.Columns)
	if err != nil {
		return err
	}
	m[IdentityKey] = o

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnDelete is called when DeleteMessageV2 is received from replication connection
func (s *ListenerJSON) OnDelete(msg *pglogrepl.DeleteMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Delete
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}

	m[SchemaKey] = rel.Namespace
	m[TableKey] = rel.RelationName

	o, err := s.readTuple(msg.OldTuple, rel.Columns)
	if err != nil {
		return err
	}
	m[IdentityKey] = o

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnLogicalDecodingMessage is called when LogicalDecodingMessageV2 is received from replication connection
func (s *ListenerJSON) OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Message
	m[TransactionalKey] = msg.Transactional
	m[PrefixKey] = msg.Prefix

	m[ContentKey] = s.encodeBinary(msg.Content)

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnTruncate is called when TruncateMessageV2 is received from replication connection
func (s *ListenerJSON) OnTruncate(msg *pglogrepl.TruncateMessageV2) error {
	// we store JSON for all relations truncated in transaction
	// this is to make sure that we either send JSON for all relations or for none (in case some marshal error occurs)
	slice := make([][]byte, len(msg.RelationIDs), len(msg.RelationIDs))
	for i := 0; i < len(msg.RelationIDs); i++ {
		m := make(map[string]any)
		m[ActionKey] = Truncate
		rel, ok := s.relations[msg.RelationIDs[i]]
		if !ok {
			return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationIDs[i])
		}
		m[SchemaKey] = rel.Namespace
		m[TableKey] = rel.RelationName

		j, err := json.Marshal(m)
		if err != nil {
			return err
		}

		slice[i] = j
	}

	for i := 0; i < len(slice); i++ {
		s.c <- slice[i]
	}
	return nil
}

// OnRelation is called when RelationMessageV2 is received from replication connection
func (s *ListenerJSON) OnRelation(msg *pglogrepl.RelationMessageV2) error {
	s.relations[msg.RelationID] = msg
	return nil
}

// OnType is called when TypeMessageV2 is received from replication connection
func (s *ListenerJSON) OnType(msg *pglogrepl.TypeMessageV2) error {
	// do nothing
	return nil
}

func (s *ListenerJSON) readTuple(t *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) (map[string]any, error) {
	if t == nil {
		return nil, nil
	}
	res := make(map[string]any)
	for i := 0; i < len(t.Columns); i++ {
		relCol := columns[i]
		col := t.Columns[i]
		err := s.writeValue(res, col, relCol)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (s *ListenerJSON) writeValue(res map[string]any, col *pglogrepl.TupleDataColumn, relCol *pglogrepl.RelationMessageColumn) error {
	switch col.DataType {
	case 'n': // null
		res[relCol.Name] = nil
	case 'u': // unchanged toast
		// do nothing
	case 't':
		val, err := s.decode(col.Data, relCol.DataType)
		if err != nil {
			return err
		}
		if relCol.DataType == pgTypeBytea {
			data, ok := val.([]byte)
			if ok && len(data) > 0 {
				res[relCol.Name] = s.encodeBinary(data)
				break
			}
		}

		res[relCol.Name] = val
	}

	return nil
}

func (s *ListenerJSON) decode(data []byte, dataType uint32) (any, error) {
	if dt, ok := s.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(s.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (s *ListenerJSON) encodeBinary(data []byte) string {
	switch s.ListenerJSONOptions.BinaryContentFormat {
	case BinaryEncodingHex:
		return hex.EncodeToString(data)
	default:
		return base64.StdEncoding.EncodeToString(data)
	}
}
