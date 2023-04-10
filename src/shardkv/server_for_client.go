package shardkv

import (
	"fmt"
	"time"
)

type OpId struct {
	ClientId    int64
	SequenceNum int64
}
type Operation struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OpType
	Key    string
	Value  string
	Err    Err
	OpId   OpId
}

func (op Operation) String() string {
	return fmt.Sprintf("opType=%v clientId=%v sequenceNum=%v key=%v value=%v Err=%v",
		op.OpType, op.OpId.ClientId, op.OpId.SequenceNum, op.Key, op.Value, op.Err)
}

func (kv *ShardKV) ProcessOperationL(op Operation) {
	shardId := key2shard(op.Key)
	DPrintf(100, "%v: start processOpL shardId=%v shardState=%v configNum=%v op.OpId=%v op.OpType=%v",
		kv, shardId, kv.shards[shardId].State, kv.shards[shardId].ConifgNum, op.OpId, op.OpType)

	if kv.shards[shardId].State != Serving && kv.shards[shardId].State != GCing {
		DPrintf(100, "%v: enter ErrWrongGroup ", kv)
		op.Err = ErrWrongGroup
		goto Done
	}

	if op.OpType != GetOp {
		if lastOp, exist := kv.shards[shardId].ClientLastOperation[op.OpId.ClientId]; exist && lastOp.OpId.SequenceNum >= op.OpId.SequenceNum {
			op = lastOp
			goto Done
		}
	}

	switch op.OpType {
	case GetOp:
		op.Err = OK
		op.Value = kv.shards[shardId].KVDB[op.Key]
	case PutOp:
		op.Err = OK
		kv.shards[shardId].KVDB[op.Key] = op.Value
	case AppendOp:
		op.Err = OK
		kv.shards[shardId].KVDB[op.Key] += op.Value
	default:
		errInfo := fmt.Sprintf("switch op.OpType(%v) is wrong", op.OpType)
		panic(errInfo)
	}

Done:
	if op.OpType != GetOp && op.Err != ErrWrongGroup {
		if lastOp, exist := kv.shards[shardId].ClientLastOperation[op.OpId.ClientId]; !exist || lastOp.OpId.SequenceNum < op.OpId.SequenceNum {
			tmpop := op
			tmpop.Key = ""
			tmpop.Value = ""
			kv.shards[shardId].ClientLastOperation[op.OpId.ClientId] = tmpop
		}
	}
	if _, isLeader := kv.rf.GetState(); isLeader {
		if _, exist := kv.taskCh[op.OpId]; !exist {
			return
		}
		DPrintf(11, "%v: next apply to client op.OpId.ClientId=%v op.OpId.SequenceNum=%v op.OpType=%v op.Err=%v op.Key=%v op.Value=%v", kv, op.OpId.ClientId, op.OpId.SequenceNum, op.OpType, op.Err, op.Key, op.Value)
		kv.taskCh[op.OpId] <- op
		DPrintf(100, "%v: done apply to client op.OpId.ClientId=%v op.OpId.SequenceNum=%v op.OpType=%v op.Err=%v ",
			kv, op.OpId.ClientId, op.OpId.SequenceNum, op.OpType, op.Err)
		delete(kv.taskCh, op.OpId)
	} else {
		//clear taskCh
		for opid, ch := range kv.taskCh {
			op := Operation{Err: ErrWrongLeader}
			DPrintf(100, "%v: next talk client I am not leader opid:%v", kv, opid)
			ch <- op
			DPrintf(100, "%v: done talk client I am not leader opid:%v", kv, opid)
		}
		kv.taskCh = make(map[OpId]chan<- Operation)
	}
	DPrintf(100, "%v: end processOpL op.OpId=%v op.OpType=%v op.Key=%v op.Value=%v", kv, op.OpId, op.OpType, op.Key, op.Value)
}

func (kv *ShardKV) RegisterTask(op Operation, ch chan<- Operation) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(0, "%v: RegisterTask acquire lock", kv)
	defer DPrintf(0, "%v: RegisterTask release lock", kv)
	DPrintf(10, "%v: RegisterTask %v", kv, op)
	if kv.killed() {
		op.Err = ErrKilled
		ch <- op
		return false
	}
	if _, exist := kv.taskCh[op.OpId]; exist {
		op.Err = ErrDuplicate
		ch <- op
		return false
	}
	_, term, isLeader := kv.rf.Start(op)
	DPrintf(100, "%v: I am isleader(%v) term(%v) in RegisterTask", kv, isLeader, term)
	if !isLeader {
		op.Err = ErrWrongLeader
		ch <- op
		return false
	}
	DPrintf(11, "%v: start raft op:%v", kv, op)
	kv.taskCh[op.OpId] = ch
	return true
}

func (kv *ShardKV) UnregisterTask(op Operation) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(0, "%v: UnregisterTask acquire lock", kv)
	defer DPrintf(0, "%v: UnregisterTask release lock", kv)
	DPrintf(11, "%v: UnregisterTask op=%v", kv, op)
	delete(kv.taskCh, op.OpId)
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// if kv.killed() {
	// 	reply.Err = ErrKilled
	// 	return
	// }
	DPrintf(100, "%v: start Get args.ClientId=%v args.SequenceNum=%v", kv, args.ClientId, args.SequenceNum)
	op := Operation{
		OpType: GetOp,
		Key:    args.Key,
		OpId:   OpId{args.ClientId, args.SequenceNum},
	}
	ch := make(chan Operation, 1)
	if kv.RegisterTask(op, ch) {
		defer kv.UnregisterTask(op)
		timer := time.NewTimer(kv.requestTimeout)
		defer timer.Stop()
		select {
		case retOp := <-ch:
			reply.Err = retOp.Err
			reply.Value = retOp.Value
		case <-timer.C:
			reply.Err = ErrTimeout
		}
	} else {
		retOp := <-ch
		reply.Err = retOp.Err
	}

	DPrintf(100, "%v: end Get args.OpType=%v args.ClientId=%v args.SequenceNum=%v reply.Err=%v", kv, GetOp, args.ClientId, args.SequenceNum, reply.Err)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// if kv.killed() {
	// 	reply.Err = ErrKilled
	// 	return
	// }
	DPrintf(100, "%v: start PutAppend args.OpType=%v args.ClientId=%v args.SequenceNum=%v ", kv, args.OpType, args.ClientId, args.SequenceNum)
	op := Operation{
		OpType: args.OpType,
		Key:    args.Key,
		Value:  args.Value,
		OpId:   OpId{args.ClientId, args.SequenceNum},
	}
	ch := make(chan Operation, 1)
	if kv.RegisterTask(op, ch) {
		defer kv.UnregisterTask(op)
		timer := time.NewTimer(kv.requestTimeout)
		defer timer.Stop()
		select {
		case retOp := <-ch:
			reply.Err = retOp.Err
		case <-timer.C:
			reply.Err = ErrTimeout
		}
	} else {
		retOp := <-ch
		reply.Err = retOp.Err
	}

	DPrintf(100, "%v: end PutAppend args.OpType=%v args.ClientId=%v args.SequenceNum=%v reply.Err=%v",
		kv, args.OpType, args.ClientId, args.SequenceNum, reply.Err)
}
