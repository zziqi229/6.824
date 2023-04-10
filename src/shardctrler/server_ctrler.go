package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs      []Config // indexed by config num
	queryTimeout time.Duration

	taskCh          map[OpId]chan<- OpReply
	clientLastReply map[int64]OpReply
}

func (sc *ShardCtrler) String() string {
	return fmt.Sprintf("[ShardCtrler %v]", sc.me)
}

func (op Op) String() string {
	switch op.OpType {
	case JoinOp:
		return fmt.Sprintf("opId=%v args=(%v,%v)", op.OpId, op.OpType, op.JoinArgs)
	case LeaveOp:
		return fmt.Sprintf("opId=%v args=(%v,%v)", op.OpId, op.OpType, op.LeaveArgs)
	case MoveOp:
		return fmt.Sprintf("opId=%v args=(%v,%v)", op.OpId, op.OpType, op.MoveArgs)
	case QueryOp:
		return fmt.Sprintf("opId=%v args=(%v,%v)", op.OpId, op.OpType, op.QueryArgs)
	default:
		errInfo := fmt.Sprintf("switch OpType %v is wrong", op.OpType)
		panic(errInfo)
	}
}
func (cfg *Config) ImplMoveL(moveList []MoveOperation) {
	for _, mo := range moveList {
		cfg.Shards[mo.ShardId] = mo.To
	}
}
func (cfg *Config) BalanceL() {
	cntGroup := len(cfg.Groups)
	if cntGroup == 0 {
		return
	}
	gidList := make([]int, 0, cntGroup)
	for gid, _ := range cfg.Groups {
		gidList = append(gidList, gid)
	}
	sort.Ints(gidList)

	cur := make(map[int]int)
	for _, gid := range cfg.Shards {
		if gid > 0 {
			cur[gid]++
		}
	}
	target := make(map[int]int)
	for _, gid := range gidList {
		target[gid] = NShards / cntGroup
	}
	last := NShards % cntGroup

	for _, gid := range gidList {
		if last <= 0 {
			break
		}
		if cur[gid] > NShards/cntGroup {
			target[gid]++
			last--
		}
	}
	for _, gid := range gidList {
		if last <= 0 {
			break
		}
		if target[gid] == NShards/cntGroup {
			target[gid]++
			last--
		}
	}

	moveList := make([]MoveOperation, 0)
	for shardId, gid := range cfg.Shards {
		if gid <= 0 {
			mo := MoveOperation{ShardId: shardId, From: -1, To: -1}
			moveList = append(moveList, mo)
		}
	}
	for _, gid := range gidList {
		if target[gid] < cur[gid] {
			// gidContained := make([]int, 0, len(cfg.Contained[gid]))
			// for shardId, _ := range cfg.Contained[gid] {
			// gidContained = append(gidContained, shardId)
			// }
			// sort.Ints(gidContained)
			cnt := cur[gid] - target[gid]
			for shardId := range cfg.Shards {
				if cfg.Shards[shardId] != gid {
					continue
				}
				if cnt <= 0 {
					break
				}
				mo := MoveOperation{ShardId: shardId, From: gid, To: -1}
				moveList = append(moveList, mo)
				cnt--
			}
		}
	}
	sort.Sort(ByShardId(moveList))
	p := 0
	for _, gid := range gidList {
		if target[gid] > cur[gid] {
			cnt := target[gid] - cur[gid]
			for ; cnt > 0; cnt-- {
				moveList[p].To = gid
				p++
			}
		}
	}
	DPrintf(11, "cfg balance cur=%v target=%v moveList=%v", cur, target, moveList)
	if p != len(moveList) {
		errInfo := fmt.Sprintf("moveOperation occur error p=%v len(moveList)=%v cfg.Groups=%v cfg.Shards=%v", p, moveList, cfg.Groups, cfg.Shards)
		panic(errInfo)
	}
	cfg.ImplMoveL(moveList)
}

func (sc *ShardCtrler) ProcessJoinL(args JoinArgs) (reply JoinReply) {
	DPrintf(11, "%v: start ProcessJoin args.ClientId=%v args.SequenceNum=%v args.Servers=%v", sc, args.ClientId, args.SequenceNum, args.Servers)
	newCfg := Config{}
	deepCopy(&newCfg, sc.configs[len(sc.configs)-1])
	newCfg.Num++
	for gid, name := range args.Servers {
		if _, exist := newCfg.Groups[gid]; exist {
			continue
		}
		newCfg.Groups[gid] = name
		delete(newCfg.GroupsDeleted, gid)
	}
	DPrintf(11, "%v: ProcessJoinL start balance", sc)
	newCfg.BalanceL()
	DPrintf(11, "%v: ProcessJoinL end balance", sc)
	sc.configs = append(sc.configs, newCfg)
	reply.Err = OK
	DPrintf(11, "%v: end ProcessJoin args.ClientId=%v args.SequenceNum=%v newCfg=%v", sc, args.ClientId, args.SequenceNum, newCfg)
	return
}
func (sc *ShardCtrler) ProcessLeaveL(args LeaveArgs) (reply LeaveReply) {
	DPrintf(11, "%v: start ProcessLeave args.ClientId=%v args.SequenceNum=%v args.Servers=%v", sc, args.ClientId, args.SequenceNum, args.GIDs)
	newCfg := Config{}
	deepCopy(&newCfg, sc.configs[len(sc.configs)-1])
	newCfg.Num++
	for _, gid := range args.GIDs {
		if _, exist := newCfg.Groups[gid]; !exist {
			continue
		}
		for shardId := range newCfg.Shards {
			if newCfg.Shards[shardId] == gid {
				newCfg.Shards[shardId] = -1
			}
		}
		newCfg.GroupsDeleted[gid] = newCfg.Groups[gid]
		delete(newCfg.Groups, gid)
	}
	DPrintf(11, "%v: ProcessLeaveL start balance", sc)
	newCfg.BalanceL()
	DPrintf(11, "%v: ProcessLeaveL end balance", sc)
	sc.configs = append(sc.configs, newCfg)
	reply.Err = OK
	DPrintf(11, "%v: end ProcessLeave args.ClientId=%v args.SequenceNum=%v newCfg=%v", sc, args.ClientId, args.SequenceNum, newCfg)
	return
}
func (sc *ShardCtrler) ProcessMoveL(args MoveArgs) (reply MoveReply) {
	DPrintf(11, "%v: start ProcessMove args.ClientId=%v args.SequenceNum=%v args.Shard=%v to args.GID=%v", sc, args.ClientId, args.SequenceNum, args.Shard, args.GID)
	newCfg := Config{}
	deepCopy(&newCfg, sc.configs[len(sc.configs)-1])
	newCfg.Num++

	moveList := []MoveOperation{
		{
			ShardId: args.Shard,
			From:    sc.configs[len(sc.configs)-1].Shards[args.Shard],
			To:      args.GID,
		},
	}
	newCfg.ImplMoveL(moveList)
	sc.configs = append(sc.configs, newCfg)
	reply.Err = OK
	DPrintf(11, "%v: end ProcessMove args.ClientId=%v args.SequenceNum=%v args.Shard=%v to args.GID=%v newCfg=%v", sc, args.ClientId, args.SequenceNum, args.Shard, args.GID, newCfg)
	return
}
func (sc *ShardCtrler) ProcessQueryL(args QueryArgs) (reply QueryReply) {
	if args.Num == -1 {
		args.Num = len(sc.configs) - 1
	}
	if args.Num >= len(sc.configs) {
		args.Num = len(sc.configs) - 1
	}
	reply.Config = sc.configs[args.Num]
	reply.Err = OK
	return
}
func (sc *ShardCtrler) ListenApplyMsg() {
	for msg := range sc.applyCh {
		//TODO
		sc.mu.Lock()
		DPrintf(0, "%v: ListenApplyMsg acquire lock", sc)
		if msg.CommandValid {
			op := msg.Command.(Op)
			opReply := OpReply{
				OpId: op.OpId,
			}
			DPrintf(11, "%v: come msg CommandIndex=%v op= {%v}", sc, msg.CommandIndex, op)
			if lastOpReply, exist := sc.clientLastReply[op.OpId.ClientId]; exist && lastOpReply.OpId.SequenceNum >= op.OpId.SequenceNum {
				opReply = lastOpReply
			} else {
				switch op.OpType {
				case JoinOp:
					reply := sc.ProcessJoinL(op.JoinArgs)
					opReply.Err = reply.Err
					opReply.JoinReply = reply
				case LeaveOp:
					reply := sc.ProcessLeaveL(op.LeaveArgs)
					opReply.Err = reply.Err
					opReply.LeaveReply = reply
				case MoveOp:
					reply := sc.ProcessMoveL(op.MoveArgs)
					opReply.Err = reply.Err
					opReply.MoveReply = reply
				case QueryOp:
					reply := sc.ProcessQueryL(op.QueryArgs)
					opReply.Err = reply.Err
					opReply.QueryReply = reply
				default:
					errInfo := fmt.Sprintf("switch OpType (%v,%v) is wrong", op.OpType, op)
					panic(errInfo)
				}
				sc.clientLastReply[op.OpId.ClientId] = opReply
			}
			if _, isLeader := sc.rf.GetState(); isLeader {
				if _, exist := sc.taskCh[op.OpId]; exist {
					DPrintf(11, "%v: next apply to client OpType=%v, clientId=%v, sequenceNum=%v, reply.Err=%v", sc, op.OpType, op.OpId.ClientId, op.OpId.SequenceNum, opReply.Err)
					sc.taskCh[op.OpId] <- opReply
					delete(sc.taskCh, op.OpId)
				}
			} else {
				//clear taskCh
				for opid, ch := range sc.taskCh {
					opReply := OpReply{
						OpId: opid,
						Err:  ErrWrongLeader,
					}
					DPrintf(100, "%v: next talk client I am not leader opid:%v", sc, opid)
					ch <- opReply
					DPrintf(100, "%v: finish talk client I am not leader opid:%v", sc, opid)
				}
				sc.taskCh = make(map[OpId]chan<- OpReply)
			}

		} else if msg.SnapshotValid {

		} else {
			errInfo := fmt.Sprintf("%v: msg.CommandValid=%v msg.SnapshotValid=%v both is false", sc, msg.CommandValid, msg.SnapshotValid)
			panic(errInfo)
		}
		DPrintf(0, "%v: ListenApplyMsg release lock", sc)
		sc.mu.Unlock()
		if sc.killed() {
			return
		}
	}
}

func (sc *ShardCtrler) RegisterTask(op Op, ch chan<- OpReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf(0, "%v: RegisterTask acquire lock", sc)
	defer DPrintf(0, "%v: RegisterTask release lock", sc)
	DPrintf(100, "%v: RegisterTask %v", sc, op)

	if sc.killed() {
		opReply := OpReply{
			OpId: op.OpId,
			Err:  ErrKilled,
		}
		ch <- opReply
		return
	}

	if _, exist := sc.taskCh[op.OpId]; exist {
		opReply := OpReply{
			OpId: op.OpId,
			Err:  ErrDuplicate,
		}
		ch <- opReply
		return
	}

	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		opReply := OpReply{
			OpId: op.OpId,
			Err:  ErrWrongLeader,
		}
		ch <- opReply
		return
	}
	DPrintf(11, "%v: start raft op:%v", sc, op)
	sc.taskCh[op.OpId] = ch
}

func (sc *ShardCtrler) UnregisterTask(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf(0, "%v: UnregisterTask acquire lock", sc)
	defer DPrintf(0, "%v: UnregisterTask release lock", sc)
	DPrintf(11, "%v: UnregisterTask op=%v", sc, op)
	delete(sc.taskCh, op.OpId)
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpId:     OpId{args.ClientId, args.SequenceNum},
		OpType:   JoinOp,
		JoinArgs: *args,
	}
	DPrintf(1, "%v: start Join args.ClientId=%v args.SequenceNum=%v args.Servers=%v", sc, args.ClientId, args.SequenceNum, args.Servers)

	ch := make(chan OpReply, 1)
	sc.RegisterTask(op, ch)
	defer sc.UnregisterTask(op)
	timer := time.NewTimer(sc.queryTimeout)
	select {
	case opReply := <-ch:
		if opReply.Err != OK {
			reply.Err = opReply.Err
		} else {
			*reply = opReply.JoinReply
		}
	case <-timer.C:
		reply.Err = ErrTimeout
	}
	DPrintf(1, "%v: end Join args.ClientId=%v args.SequenceNum=%v args.Servers=%v reply.Err=%v", sc, args.ClientId, args.SequenceNum, args.Servers, reply.Err)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpId:      OpId{args.ClientId, args.SequenceNum},
		OpType:    LeaveOp,
		LeaveArgs: *args,
	}
	DPrintf(1, "%v: start Leave args.ClientId=%v args.SequenceNum=%v args.GIDs=%v", sc, args.ClientId, args.SequenceNum, args.GIDs)

	ch := make(chan OpReply, 1)
	sc.RegisterTask(op, ch)
	defer sc.UnregisterTask(op)
	timer := time.NewTimer(sc.queryTimeout)
	select {
	case opReply := <-ch:
		if opReply.Err != OK {
			reply.Err = opReply.Err
		} else {
			*reply = opReply.LeaveReply
		}
	case <-timer.C:
		reply.Err = ErrTimeout
	}
	DPrintf(1, "%v: end Leave args.ClientId=%v args.SequenceNum=%v args.GIDs=%v reply.Err=%v", sc, args.ClientId, args.SequenceNum, args.GIDs, reply.Err)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpId:     OpId{args.ClientId, args.SequenceNum},
		OpType:   MoveOp,
		MoveArgs: *args,
	}
	DPrintf(1, "%v: start Move args.ClientId=%v args.SequenceNum=%v args.Shard=%v args.GID=%v", sc, args.ClientId, args.SequenceNum, args.Shard, args.GID)

	ch := make(chan OpReply, 1)
	sc.RegisterTask(op, ch)
	defer sc.UnregisterTask(op)
	timer := time.NewTimer(sc.queryTimeout)
	select {
	case opReply := <-ch:
		if opReply.Err != OK {
			reply.Err = opReply.Err
		} else {
			*reply = opReply.MoveReply
		}
	case <-timer.C:
		reply.Err = ErrTimeout
	}
	DPrintf(1, "%v: end Move args.ClientId=%v args.SequenceNum=%v args.Shard=%v args.GID=%v reply.Err=%v", sc, args.ClientId, args.SequenceNum, args.Shard, args.GID, reply.Err)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpId:      OpId{args.ClientId, args.SequenceNum},
		OpType:    QueryOp,
		QueryArgs: *args,
	}
	DPrintf(1, "%v: start Query args.ClientId=%v args.SequenceNum=%v args.Num=%v", sc, args.ClientId, args.SequenceNum, args.Num)

	ch := make(chan OpReply, 1)
	sc.RegisterTask(op, ch)
	defer sc.UnregisterTask(op)
	timer := time.NewTimer(sc.queryTimeout)
	select {
	case opReply := <-ch:
		if opReply.Err != OK {
			reply.Err = opReply.Err
		} else {
			*reply = opReply.QueryReply
		}
	case <-timer.C:
		reply.Err = ErrTimeout
	}
	DPrintf(1, "%v: end Query args.ClientId=%v args.SequenceNum=%v args.Num=%v reply.Err=%v reply.Config=%v", sc, args.ClientId, args.SequenceNum, args.Num, reply.Err, reply.Config)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool {
	return sc.rf.Killed()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = []Config{
		{
			Num:           0,
			Shards:        [NShards]int{},
			Groups:        make(map[int][]string),
			GroupsDeleted: make(map[int][]string),
		},
	}
	sc.queryTimeout = time.Duration(500 * time.Millisecond)
	sc.clientLastReply = make(map[int64]OpReply)
	sc.taskCh = make(map[OpId]chan<- OpReply)

	go sc.ListenApplyMsg()

	testTimer := time.NewTimer(time.Second * 50)
	go sc.alive(testTimer)
	go func() {
		<-testTimer.C
		if sc.killed() {
			return
		}
		tmpInfo := fmt.Sprintf("%v: bomb", sc)
		panic(tmpInfo)
	}()
	return sc
}

func (sc *ShardCtrler) alive(testTimer *time.Timer) {
	for !sc.killed() {
		DPrintf(100, "%v: alive wait acquire lock", sc)
		sc.mu.Lock()
		testTimer.Reset(time.Second * 50)
		DPrintf(100, "%v: I am alive\n", sc)
		sc.mu.Unlock()
		time.Sleep(time.Second)
	}
	testTimer.Stop()
}
