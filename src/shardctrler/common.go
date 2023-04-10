package shardctrler

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num           int              // config number
	Shards        [10]int          // shard -> gid
	Groups        map[int][]string // gid -> sservers[]
	GroupsDeleted map[int][]string
	// Contained     map[int]map[int]bool
}

func (cfg Config) String() string {
	return fmt.Sprintf("{Num=%v,Shards=%v}", cfg.Num, cfg.Shards)
}
func (cfg1 *Config) CopyConfig() (cfg2 Config) {
	cfg2.Num = cfg1.Num
	cfg2.Shards = cfg1.Shards

	cfg2.Groups = make(map[int][]string)
	for gid, servers := range cfg1.Groups {
		cfg2.Groups[gid] = append([]string{}, servers...)
	}
	return
}
func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

type MoveOperation struct {
	ShardId int
	From    int
	To      int
}
type ByShardId []MoveOperation

func (mos ByShardId) Len() int           { return len(mos) }
func (mos ByShardId) Swap(i, j int)      { mos[i], mos[j] = mos[j], mos[i] }
func (mos ByShardId) Less(i, j int) bool { return mos[i].ShardId < mos[j].ShardId }

type Err string

const (
	OK             Err = "OK"
	ErrKilled      Err = "ErrKilled"
	ErrTimeout     Err = "ErrTimeout"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrDuplicate   Err = "ErrDuplicate"
)

type OpType string

const (
	JoinOp  OpType = "JoinOp"
	LeaveOp OpType = "LeaveOp"
	MoveOp  OpType = "MoveOp"
	QueryOp OpType = "QueryOp"
)

type OpId struct {
	ClientId    int64
	SequenceNum int64
}

func (opId OpId) String() string {
	return fmt.Sprintf("(%v:%v)", opId.ClientId, opId.SequenceNum)
}

type Op struct {
	OpId      OpId
	OpType    OpType
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs
	QueryArgs QueryArgs
}
type OpReply struct {
	OpId       OpId
	Err        Err
	OpType     OpType
	JoinReply  JoinReply
	LeaveReply LeaveReply
	MoveReply  MoveReply
	QueryReply QueryReply
}

type JoinArgs struct {
	Servers     map[int][]string // new GID -> servers mappings
	ClientId    int64
	SequenceNum int64
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs []int

	ClientId    int64
	SequenceNum int64
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientId    int64
	SequenceNum int64
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num int // desired config number

	ClientId    int64
	SequenceNum int64
}

type QueryReply struct {
	Err    Err
	Config Config
}
