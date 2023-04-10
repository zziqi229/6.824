package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu          sync.Mutex
	clientId    int64
	sequenceNum int64

	prefer  int
	timeout time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.prefer = 0
	ck.timeout = 500 * time.Millisecond
	return ck
}

func (ck *Clerk) call(callOne func(serverId int) (bool, Err, interface{})) interface{} {
	prefer := ck.prefer
	type result struct {
		serverId int
		err      Err
		reply    interface{}
	}
	ch := make(chan result, 1)
	timer := time.NewTimer(ck.timeout)
	defer timer.Stop()

	done := atomic.Bool{}
	done.Store(false)

	var res result
	for i := 0; ; i++ {
		serverId := (prefer + i) % len(ck.servers)
		go func() {
			ok, Err, reply := callOne(serverId)
			if done.Load() {
				return
			}
			if ok {
				ch <- result{serverId, Err, reply}
				DPrintf(10, "[Client %v]: end call serverId=%v prefer=%v Err=%v", ck.clientId, serverId, prefer, Err)
			}
		}()
		select {
		case res = <-ch:
			if res.err == OK {
				done.Store(true)
				goto Done
			}
		case <-timer.C:
			timer.Reset(ck.timeout)
		}
	}
Done:
	ck.prefer = res.serverId
	return res.reply
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum++
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum
	// try each known server.
	callOne := func(serverId int) (bool, Err, interface{}) {
		srv := ck.servers[serverId]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		return ok, reply.Err, reply
	}
	reply := ck.call(callOne).(QueryReply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	DPrintf(900, "start Join %v", servers)
	defer DPrintf(900, "end Join %v", servers)

	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum++
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum
	callOne := func(serverId int) (bool, Err, interface{}) {
		srv := ck.servers[serverId]
		var reply JoinReply
		ok := srv.Call("ShardCtrler.Join", args, &reply)
		return ok, reply.Err, reply
	}
	ck.call(callOne)
}

func (ck *Clerk) Leave(gids []int) {
	DPrintf(900, "start Leave %v", gids)
	defer DPrintf(900, "end Leave %v", gids)
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum++
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum

	callOne := func(serverId int) (bool, Err, interface{}) {
		srv := ck.servers[serverId]
		var reply LeaveReply
		ok := srv.Call("ShardCtrler.Leave", args, &reply)
		return ok, reply.Err, reply
	}
	ck.call(callOne)
}

func (ck *Clerk) Move(shard int, gid int) {
	DPrintf(900, "start move shard=%v gid=%v", shard, gid)
	defer DPrintf(900, "end move shard=%v gid=%v", shard, gid)
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum++
	args.ClientId = ck.clientId
	args.SequenceNum = ck.sequenceNum

	callOne := func(serverId int) (bool, Err, interface{}) {
		srv := ck.servers[serverId]
		var reply MoveReply
		ok := srv.Call("ShardCtrler.Move", args, &reply)
		return ok, reply.Err, reply
	}
	ck.call(callOne)
}
