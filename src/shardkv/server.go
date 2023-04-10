package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Shard struct {
	KVDB                map[string]string
	ClientLastOperation map[int64]Operation
	ConifgNum           int
	State               ShardState
}

func (sd *Shard) String() string {
	return fmt.Sprintf("configNum=%v, state=%v, KVDB=%v", sd.ConifgNum, sd.State, sd.KVDB)
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	taskCh         map[OpId]chan<- Operation
	requestTimeout time.Duration

	sc      *shardctrler.Clerk
	configs []shardctrler.Config //persistent
	shards  []Shard              //persistent

	prefer map[int]int
	Groups map[int][]string
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("[ShardKV gid=%v me=%v]", kv.gid, kv.me)
}
func (kv *ShardKV) EncodeSnapshot() []byte {
	errInfo := ""
	DPrintf(100, "%v: send snapshot", kv)
	shardInfo := "Shard State: "
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		shardInfo += fmt.Sprintf("%v:%v ", shardId, kv.shards[shardId].State)
		if kv.shards[shardId].State == Idle && (kv.shards[shardId].KVDB != nil || kv.shards[shardId].ClientLastOperation != nil) {
			errInfo += fmt.Sprintf("shardId=%v state=%v KVDB=%v lastOp=%v",
				shardId, kv.shards[shardId].State, kv.shards[shardId].KVDB, kv.shards[shardId].ClientLastOperation)
		}
		if kv.shards[shardId].State == Idle {
			kv.shards[shardId].KVDB = nil
			kv.shards[shardId].ClientLastOperation = nil
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.configs)
	configSize := len(w.Bytes())
	e.Encode(kv.shards)
	shardsSize := len(w.Bytes()) - configSize
	data := w.Bytes()
	DPrintf(200, "%v: shardInfo=%v\nconfig size=%v shardsSize=%v", kv, shardInfo, configSize, shardsSize)
	if len(errInfo) > 0 {
		panic(errInfo)
		// DPrintf(999, "%v: error %v", kv, errInfo)
	}
	return data
}
func (kv *ShardKV) LoadSnapshot(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kv.configs = []shardctrler.Config{}
	kv.shards = []Shard{}
	if d.Decode(&kv.configs) != nil ||
		d.Decode(&kv.shards) != nil {
		//   error...
		errInfo := fmt.Sprintf("%v: LoadSnapshot decode error\n", kv)
		panic(errInfo)
	}
	DPrintf(100, "%v: load snapshot", kv)
	// for i := 0; i < shardctrler.NShards; i++ {
	// 	DPrintf(100, "%v: shardId=%v state=%v", kv, i, kv.shards[i].State)
	// 	if kv.shards[i].State == Serving || kv.shards[i].State == GCing {
	// 		DPrintf(100, "%v:shardId=%v KVDB=%v lastOp=%v", kv, i, kv.shards[i].KVDB, kv.shards[i].ClientLastOperation)
	// 	}
	// }
	shardInfo := "Shard State: "
	errInfo := ""
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		shardInfo += fmt.Sprintf("%v:%v ", shardId, kv.shards[shardId].State)
		if kv.shards[shardId].State == Idle && (kv.shards[shardId].KVDB != nil || kv.shards[shardId].ClientLastOperation != nil) {
			errInfo += fmt.Sprintf("shardId=%v state=%v KVDB=%v lastOp=%v",
				shardId, kv.shards[shardId].State, kv.shards[shardId].KVDB, kv.shards[shardId].ClientLastOperation)
		}
	}
	DPrintf(200, "%v: shardInfo=%v\nconfig size=+shardsSize=%v", kv, shardInfo, len(data))
	if len(errInfo) > 0 {
		panic(errInfo)
		// DPrintf(999, "%v: error %v", kv, errInfo)
	}
	return true
}

func (kv *ShardKV) ListenMessage() {
	for msg := range kv.applyCh {
		DPrintf(11, "%v: come a msg", kv)
		kv.mu.Lock()
		DPrintf(0, "%v: ListenMessage acquire lock", kv)
		if msg.CommandValid {
			if v, ok := msg.Command.(int); ok {
				DPrintf(100, "%v: pass empty %v", kv, v)
				kv.mu.Unlock()
				continue
			}
			DPrintf(11, "%v: come a command %T %v", kv, msg.Command, msg.Command)
			switch v := msg.Command.(type) {
			case Operation:
				kv.ProcessOperationL(v)
			case shardctrler.Config:
				kv.ProcessNewConfigL(v)
			case RequestGetShardArgs:
				kv.ProcessRequestGetShardL(v)
			case RequestPutShardArgs:
				kv.ProcessRequestPutShardL(v)
			case RequestGCShardArgs:
				kv.ProcessGCShardL(v)
			case RequestGCAckShardArgs:
				kv.ProcessGCAckShardL(v)
			default:
				errInfo := fmt.Sprintf("%v: ListenMessage Command type(%T) occur wrong", kv, msg.Command)
				panic(errInfo)
			}

			if _, isLeader := kv.rf.GetState(); isLeader &&
				kv.maxraftstate > 0 && float64(kv.persister.RaftStateSize()) > float64(kv.maxraftstate)*0.95 {
				index := msg.CommandIndex
				data := kv.EncodeSnapshot()
				kv.rf.Snapshot(index, data)
				DPrintf(100, "%v send snapshot index=%v", kv, index)
			}
		} else if msg.SnapshotValid {
			DPrintf(100, "%v: come snapshot SnapshotIndex=%v ", kv, msg.SnapshotIndex)
			data := msg.Snapshot
			kv.LoadSnapshot(data)
		} else {
			errInfo := fmt.Sprintf("%v: msg.CommandValid=%v msg.SnapshotValid=%v both is false", kv, msg.CommandValid, msg.SnapshotValid)
			panic(errInfo)
		}
		DPrintf(0, "%v: ListenMessage release lock", kv)
		kv.mu.Unlock()
		if kv.killed() {
			return
		}
	}
}
func (kv *ShardKV) StartEmptyCommand() {
	for i := 1; !kv.killed(); i++ {
		kv.rf.Start(i)
		time.Sleep(500 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	DPrintf(100, "%v is kill", kv)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	return kv.rf.Killed()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Operation{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(RequestGetShardArgs{})
	labgob.Register(RequestPutShardArgs{})
	labgob.Register(RequestGCShardArgs{})
	labgob.Register(RequestGCAckShardArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister
	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	DPrintf(100, "%v: StartServer", kv)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.taskCh = make(map[OpId]chan<- Operation)
	kv.requestTimeout = 20 * time.Second

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.prefer = make(map[int]int)

	if !kv.LoadSnapshot(kv.persister.ReadSnapshot()) {
		kv.configs = []shardctrler.Config{kv.sc.Query(0)}
		kv.shards = make([]Shard, shardctrler.NShards)
		for i := 0; i < len(kv.shards); i++ {
			kv.shards[i] = Shard{
				KVDB:                nil,
				ClientLastOperation: nil,
				ConifgNum:           0,
				State:               Idle,
			}
		}
	}

	go kv.ListenMessage()
	go kv.ConfigApplier()
	go kv.MonitorNewConfig()
	go kv.MonitorGCShard()
	go kv.StartEmptyCommand()
	testTimer := time.NewTimer(time.Second * 10)
	go kv.alive(testTimer)
	go func() {
		<-testTimer.C
		if kv.killed() {
			return
		}
		tmpInfo := fmt.Sprintf("%v: bomb", kv)
		panic(tmpInfo)
	}()
	return kv
}
func (kv *ShardKV) alive(testTimer *time.Timer) {
	for !kv.killed() {
		DPrintf(100, "%v: alive wait acquire lock", kv)
		kv.mu.Lock()
		testTimer.Reset(time.Second * 50)
		term, isLeader := kv.rf.GetState()
		DPrintf(100, "%v: I am alive  term=%v, isLeader=%v", kv, term, isLeader)
		if term > 10 && false {
			panic("term too big")
		}
		kv.mu.Unlock()
		time.Sleep(time.Second)
	}
	testTimer.Stop()
}
