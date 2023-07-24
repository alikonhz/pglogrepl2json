package pglogrepl2json

import "github.com/jackc/pglogrepl"

// ReplicationListener is a target data store where Replicator sends logical decoding messages
type ReplicationListener interface {
	OnTxBegin(msg *pglogrepl.BeginMessage) error
	OnTxCommit(msg *pglogrepl.CommitMessage) error
	OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error
	OnStreamStop(msg *pglogrepl.StreamStopMessageV2) error
	OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error
	OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error
	OnInsert(msg *pglogrepl.InsertMessageV2) error
	OnUpdate(msg *pglogrepl.UpdateMessageV2) error
	OnDelete(msg *pglogrepl.DeleteMessageV2) error
	OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error
	OnTruncate(msg *pglogrepl.TruncateMessageV2) error
	OnRelation(msg *pglogrepl.RelationMessageV2) error
	OnOrigin(msg *pglogrepl.OriginMessage) error
	OnType(msg *pglogrepl.TypeMessageV2) error
}
