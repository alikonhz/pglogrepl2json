package pglogrepl2json

import "github.com/jackc/pglogrepl"

// ReplicationListener is a target data store where Replicator sends logical decoding messages
type ReplicationListener interface {

	// OnTxBegin is called when BeginMessage is received from replication connection
	OnTxBegin(msg *pglogrepl.BeginMessage) error

	// OnTxCommit is called when CommitMessage is received from replication connection
	OnTxCommit(msg *pglogrepl.CommitMessage) error

	// OnStreamStart is called when StreamStartMessageV2 is received from replication connection
	OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error

	// OnStreamStop is called when StreamStopMessageV2 is received from replication connection
	OnStreamStop(msg *pglogrepl.StreamStopMessageV2) error

	// OnStreamCommit is called when StreamCommitMessageV2 is received from replication connection
	OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error

	// OnStreamAbort is called when StreamAbortMessageV2 is received from replication connection
	OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error

	// OnInsert is called when InsertMessageV2 is received from replication connection
	OnInsert(msg *pglogrepl.InsertMessageV2) error

	// OnUpdate is called when UpdateMessageV2 is received from replication connection
	OnUpdate(msg *pglogrepl.UpdateMessageV2) error

	// OnDelete is called when DeleteMessageV2 is received from replication connection
	OnDelete(msg *pglogrepl.DeleteMessageV2) error

	// OnLogicalDecodingMessage is called when LogicalDecodingMessageV2 is received from replication connection
	OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error

	// OnTruncate is called when TruncateMessageV2 is received from replication connection
	OnTruncate(msg *pglogrepl.TruncateMessageV2) error

	// OnRelation is called when RelationMessageV2 is received from replication connection
	OnRelation(msg *pglogrepl.RelationMessageV2) error

	// OnOrigin is called when OriginMessage is received from replication connection
	OnOrigin(msg *pglogrepl.OriginMessage) error

	// OnType is called when TypeMessageV2 is received from replication connection
	OnType(msg *pglogrepl.TypeMessageV2) error
}
