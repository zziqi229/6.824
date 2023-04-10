package shardkv

import (
	"bytes"
	"encoding/gob"
)

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrKilled      Err = "ErrKilled"
	ErrDuplicate   Err = "ErrDuplicate"
	ErrTimeout     Err = "ErrTimeout"
	ErrWrongGroup  Err = "ErrWrongGroup"
	// ErrNotReady    Err = "ErrNotReady"
)

type OpType string

const (
	PutOp    OpType = "PutOp"
	AppendOp OpType = "AppendOp"
	GetOp    OpType = "GetOp"
)

type ShardState string

const (
	Serving   ShardState = "Serving"
	Pulling   ShardState = "Pulling"
	BePulling ShardState = "BePulling"
	GCing     ShardState = "GCing"
	Idle      ShardState = "Idle"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	OpType OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Err   Err
	Value string
}
