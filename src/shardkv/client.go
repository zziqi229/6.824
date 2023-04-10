package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu          sync.Mutex
	clientId    int64
	sequenceNum int64

	prefer  map[int]int
	timeout time.Duration
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("[Clerk %v]", ck.clientId)
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.config = ck.sm.Query(-1)

	ck.timeout = 500 * time.Millisecond
	ck.prefer = make(map[int]int)
	DPrintf(100, "%v: MakeClerk", ck)
	return ck
}
func (ck *Clerk) callShardServer(gid int, callOne func(string) (bool, Err, interface{})) (bool, Err, interface{}) {
	ck.mu.Lock()
	prefer := ck.prefer[gid]
	servers, exist := ck.config.Groups[gid]
	if !exist {
		ck.mu.Unlock()
		return false, "", nil
	}
	ck.mu.Unlock()
	DPrintf(100, "%v: callShardServer start servers=%v", ck, servers)

	type result struct {
		serverId int
		ok       bool
		err      Err
		reply    interface{}
	}
	ch := make(chan result, len(servers))
	timer := time.NewTimer(100 * ck.timeout)
	defer timer.Stop()

	taskNum := len(servers)
	var res result
	for i := 0; i < len(servers); i++ {
		i := i
		serverId := (prefer + i) % len(servers)
		go func() {
			DPrintf(100, "%v: callone servers[serverId]=%v i=%v", ck, servers[serverId], i)
			ok, err, reply := callOne(servers[serverId])
			DPrintf(100, "%v: callone servers[serverId]=%v result ok=%v,err=%v i=%v", ck, servers[serverId], ok, err, i)
			ch <- result{serverId, ok, err, reply}
		}()

		select {
		case res = <-ch:
			DPrintf(100, "%v: callShardServer get chan i=%v err=%v", ck, i, res.err)
			taskNum--
			if res.ok && (res.err == OK || res.err == ErrWrongGroup) {
				goto Done
			}
			time.Sleep(ck.timeout)
			for len(timer.C) > 0 {
				<-timer.C
			}
			timer.Reset(100 * ck.timeout)
		case <-timer.C:
			DPrintf(100, "%v: callShardServer time out i=%v", ck, i)
			timer.Reset(100 * ck.timeout)
		}
	}
	for ; taskNum > 0; taskNum-- {
		res = <-ch
		if res.ok && (res.err == OK || res.err == ErrWrongGroup) {
			goto Done
		}
	}
	DPrintf(100, "%v: callShardServer end servers=%v all server fail", ck, servers)
	return false, "", nil
Done:
	ck.mu.Lock()
	ck.prefer[gid] = res.serverId
	ck.mu.Unlock()
	DPrintf(100, "%v: callShardServer end servers[serverId]=%v res.err=%v", ck, servers[res.serverId], res.err)
	return true, res.err, res.reply
}
func (ck *Clerk) call(shardId int, callOne func(string) (bool, Err, interface{})) interface{} {
	DPrintf(100, "%v: call start shardID=%v", ck, shardId)
	ck.mu.Lock()
	for ck.config.Num <= 0 {
		ck.config = ck.sm.Query(-1)
	}
	ck.mu.Unlock()
	type result struct {
		ok    bool
		err   Err
		reply interface{}
	}
	ch := make(chan result, 1)
	timeout := 100 * ck.timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := &atomic.Bool{}
	done.Store(false)

	killed := &atomic.Bool{}
	killed.Store(false)

	go func() { //MonitorNewConfig
		for !done.Load() {
			time.Sleep(4 * ck.timeout)
			nextCfg := ck.sm.Query(-1)
			ck.mu.Lock()
			if nextCfg.Num != ck.config.Num {
				ck.config = nextCfg
			}
			ck.mu.Unlock()
		}
	}()

	var res result
	for i := 0; ; i++ {
		ck.mu.Lock()
		go func(gid int) {
			DPrintf(100, "%v: call go shardServer i=%v", ck, i)
			ok, err, reply := ck.callShardServer(gid, callOne)
			if done.Load() {
				return
			}
			ch <- result{ok, err, reply}
		}(ck.config.Shards[shardId])
		ck.mu.Unlock()
		select {
		case res = <-ch:
			DPrintf(100, "%v: call get chan ok=%v err=%v", ck, res.ok, res.err)
			if res.err == OK {
				done.Store(true)
				goto Done
			}
			time.Sleep(ck.timeout)
			for len(timer.C) > 0 {
				<-timer.C
			}
			timer.Reset(timeout)
		case <-timer.C:
			DPrintf(100, "%v: call time out", ck)
			timer.Reset(timeout)
		}
	}
Done:
	DPrintf(100, "%v: call end shardID=%v res.err=%v", ck, shardId, res.err)
	return res.reply
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ch := make(chan bool)
	DPrintf(50, "%v: start Get key=%v", ck, key)
	args := GetArgs{}
	args.Key = key

	ck.sequenceNum++
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum
	go ck.bomb(args.ClientId, args.SequenceNum, ch)

	shardId := key2shard(key)
	callOne := func(server string) (bool, Err, interface{}) {
		srv := ck.make_end(server)
		var reply GetReply
		ok := srv.Call("ShardKV.Get", &args, &reply)
		DPrintf(99, "%v: get key=%v value=%v", ck, args.Key, reply.Value)
		return ok, reply.Err, reply
	}
	reply := ck.call(shardId, callOne).(GetReply)
	ch <- true
	return reply.Value
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	ch := make(chan bool)

	DPrintf(50, "%v: start PutAppend %v key=%v value=%v", ck, opType, key, value)
	defer DPrintf(50, "%v: end PutAppend %v key=%v value=%v", ck, opType, key, value)
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.OpType = opType
	ck.sequenceNum++
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum
	go ck.bomb(args.ClientId, args.SequenceNum, ch)

	shardId := key2shard(key)
	callOne := func(server string) (bool, Err, interface{}) {
		srv := ck.make_end(server)
		var reply PutAppendReply
		ok := srv.Call("ShardKV.PutAppend", &args, &reply)
		DPrintf(99, "%v: %v key=%v value=%v", ck, args.OpType, args.Key, args.Value)
		return ok, reply.Err, reply
	}
	reply := ck.call(shardId, callOne).(PutAppendReply)
	_ = reply
	ch <- true
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}

func (ck *Clerk) bomb(clientId, seq int64, ch <-chan bool) {
	timer := time.NewTimer(1500 * time.Second)
	select {
	case <-timer.C:
		errInfo := fmt.Sprintf("%v: clientId=%v seq=%v bomb", ck, clientId, seq)
		panic(errInfo)
	case <-ch:
		break
	}
	timer.Stop()
}
