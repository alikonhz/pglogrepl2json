package listeners

import (
	"encoding/json"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	errRelationNotFound = "relation not found"
)

const (
	commit = `{ "action": "C" }`
)

type ListenerJSON struct {
	c         chan string
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
}

// MustCreateNewJson creates a new JSON listener
// it panics if c channel is nil
func MustCreateNewJson(c chan string) *ListenerJSON {
	if c == nil {
		panic("channel is nil")
	}
	return &ListenerJSON{
		c:         c,
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
	}
}

func (s *ListenerJSON) OnOrigin(msg *pglogrepl.OriginMessage) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnTxBegin(msg *pglogrepl.BeginMessage) error {
	m := make(map[string]any)
	m["action"] = "B"
	m["xid"] = msg.Xid

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- string(j)

	return nil
}

func (s *ListenerJSON) OnTxCommit(msg *pglogrepl.CommitMessage) error {
	s.c <- commit
	return nil
}

func (s *ListenerJSON) OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnStreamStop(msg *pglogrepl.StreamStopMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnInsert(msg *pglogrepl.InsertMessageV2) error {
	m := make(map[string]any)
	m["action"] = "I"
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}
	m["schema"] = rel.Namespace
	m["table"] = rel.RelationName
	v, err := s.readTuple(msg.Tuple, rel.Columns)
	if err != nil {
		return err
	}

	m["new"] = v

	j, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- string(j)

	return nil
}

func (s *ListenerJSON) OnUpdate(msg *pglogrepl.UpdateMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnDelete(msg *pglogrepl.DeleteMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnTruncate(msg *pglogrepl.TruncateMessageV2) error {
	//TODO implement me
	return nil
}

func (s *ListenerJSON) OnRelation(msg *pglogrepl.RelationMessageV2) error {
	s.relations[msg.RelationID] = msg
	return nil
}

func (s *ListenerJSON) OnType(msg *pglogrepl.TypeMessageV2) error {
	//TODO implement me
	panic("implement me")
}

func (s *ListenerJSON) readTuple(t *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) (map[string]any, error) {
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
