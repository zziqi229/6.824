package shardkv

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"6.824/shardctrler"
)

type RequestGetShardArgs struct {
	NextConfigNum int
	SrcGid        int
	DstGid        int
	// SrcServers    []string
	// DstServers    []string
	ShardId int
}
type RequestGetShardReply struct {
	OK bool
}

func (argsGet RequestGetShardArgs) String() string {
	return fmt.Sprintf("NextConfigNum=%v, SrcGid=%v, DstGid=%v, ShardId=%v", argsGet.NextConfigNum, argsGet.SrcGid, argsGet.DstGid, argsGet.ShardId)
}

type RequestPutShardArgs struct {
	NextConfigNum int
	SrcGid        int
	DstGid        int
	// SrcServers             []string
	// DstServers             []string
	ShardId                int
	ShardKVDB              map[string]string
	ShardClientLastRequest map[int64]Operation
}
type RequestPutShardReply struct {
	OK bool
}

func (argsPut RequestPutShardArgs) String() string {
	return fmt.Sprintf("NextConfigNum=%v, SrcGid=%v, DstGid=%v, ShardId=%v", argsPut.NextConfigNum, argsPut.SrcGid, argsPut.DstGid, argsPut.ShardId)
}

type RequestGCShardArgs struct {
	CurrentConfigNum int
	ShardId          int
	FormerGid        int
	CurrentGid       int
	// FormerServers    []string
	// CurrentServers   []string
}

type RequestGCShardReply struct {
	OK bool
}

type RequestGCAckShardArgs RequestGCShardArgs
type RequestGCAckShardReply RequestGCShardReply

func (args *RequestGCShardArgs) String() string {
	return fmt.Sprintf("CurrentConfigNum=%v, ShardId=%v, FormerGid=%v, CurrentGid=%v",
		args.CurrentConfigNum, args.ShardId, args.FormerGid, args.CurrentGid)

}
func (args *RequestGCAckShardArgs) String() string {
	return fmt.Sprintf("CurrentConfigNum=%v, ShardId=%v, FormerGid=%v, CurrentGid=%v",
		args.CurrentConfigNum, args.ShardId, args.FormerGid, args.CurrentGid)

}
func (kv *ShardKV) RequestGCShard(args *RequestGCShardArgs, reply *RequestGCShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	reply.OK = isLeader
	if isLeader {
		DPrintf(13, "%v: RequestGCShard  args=%v", kv, args)
	}
}
func (kv *ShardKV) RequestGCAckShard(args *RequestGCAckShardArgs, reply *RequestGCAckShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	reply.OK = isLeader
	if isLeader {
		DPrintf(13, "%v: RequestGCAckShard  args=%v", kv, args)
	}
}
func (kv *ShardKV) RequestGetShard(args *RequestGetShardArgs, reply *RequestGetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(*args)
	reply.OK = isLeader
	if isLeader {
		DPrintf(13, "%v: RequestGetShard  argsGet=%v", kv, args)
	}
}

func (kv *ShardKV) RequestPutShard(args *RequestPutShardArgs, reply *RequestPutShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.NextConfigNum <= kv.shards[args.ShardId].ConifgNum {
		reply.OK = false
		return
	}
	_, _, isLeader := kv.rf.Start(*args)
	reply.OK = isLeader
	if isLeader {
		DPrintf(13, "%v: RequestPutShard argsPut=%v", kv, args)
	}
}
func (kv *ShardKV) call(gid int, callOne func(string) (bool, interface{})) interface{} {
	kv.mu.Lock()
	servers := kv.Gid2ServersL(gid)
	prefer := kv.prefer[gid]
	kv.mu.Unlock()
	type result struct {
		serverId int
		ok       bool
		reply    interface{}
	}
	ch := make(chan result, len(servers))
	timer := time.NewTimer(100 * time.Millisecond)
	var res result
	taskNum := len(servers)
	for i := 0; i < len(servers); i++ {
		serverId := (prefer + i) % len(servers)
		go func(serverId int) {
			ok, reply := callOne(servers[serverId])
			ch <- result{serverId, ok, reply}
		}(serverId)
		select {
		case res = <-ch:
			taskNum--
			if res.ok {
				goto Done
			}
		case <-timer.C:
			timer.Reset(100 * time.Millisecond)
		}
	}
	for ; taskNum > 0; taskNum-- {
		res = <-ch
		if res.ok {
			goto Done
		}
	}
	DPrintf(100, "%v: call cannot connect to %v", kv, gid)
	return nil
Done:
	kv.mu.Lock()
	kv.prefer[gid] = res.serverId
	kv.mu.Unlock()
	return res.reply
}
func (kv *ShardKV) startRequestGetShard(argsGet RequestGetShardArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	DPrintf(100, "%v: startRequestGetShard argsGet=%v", kv, argsGet)
	shardId := argsGet.ShardId
	if kv.shards[shardId].ConifgNum >= argsGet.NextConfigNum {
		DPrintf(13, "%v: skip startRequestGetShard kv.shards[argsGet.ShardId].ConifgNum(%v) >= argsGet.NextConfigNum(%v)", kv,
			kv.shards[argsGet.ShardId].ConifgNum, argsGet.NextConfigNum)
		return
	}
	if argsGet.SrcGid <= 0 { //cold start
		argsPut := RequestPutShardArgs{
			NextConfigNum:          1,
			SrcGid:                 0,
			DstGid:                 kv.gid,
			ShardId:                argsGet.ShardId,
			ShardKVDB:              make(map[string]string),
			ShardClientLastRequest: make(map[int64]Operation),
		}
		kv.rf.Start(argsPut)
		return
	}

	callOne := func(server string) (bool, interface{}) {
		var reply RequestGetShardReply
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.RequestGetShard", &argsGet, &reply)
		return ok && reply.OK, reply
	}
	go kv.call(argsGet.SrcGid, callOne)
}
func (kv *ShardKV) startRequestPutShardKVDB(argsPut RequestPutShardArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	DPrintf(100, "%v: startRequestPutShardKVDB %v", kv, argsPut)
	callOne := func(server string) (bool, interface{}) {
		var reply RequestPutShardReply
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.RequestPutShard", &argsPut, &reply)
		return ok && reply.OK, reply
	}
	go kv.call(argsPut.DstGid, callOne)
}
func (kv *ShardKV) startRequestGCShard(args RequestGCShardArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	DPrintf(100, "%v: startRequestGCShard %v", kv, args)
	if args.FormerGid <= 0 {
		argsGCAck := RequestGCAckShardArgs(args)
		_, _, isLeader := kv.rf.Start(argsGCAck)
		DPrintf(20, "%v: skip startRequestGCShard %v isLeader=%v", kv, args, isLeader)
		return
	}

	callOne := func(server string) (bool, interface{}) {
		var reply RequestGCShardReply
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.RequestGCShard", &args, &reply)
		return ok && reply.OK, reply
	}
	go kv.call(args.FormerGid, callOne)
}
func (kv *ShardKV) startRequestGCAckShard(args RequestGCAckShardArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	DPrintf(100, "%v: startRequestGCAckShard %v", kv, args)
	callOne := func(server string) (bool, interface{}) {
		var reply RequestGCAckShardReply
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.RequestGCAckShard", &args, &reply)
		return ok && reply.OK, reply
	}
	go kv.call(args.CurrentGid, callOne)
}
func (kv *ShardKV) ConfigApplier() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		minCfgNum := math.MaxInt
		for shardId := 0; shardId < shardctrler.NShards; shardId++ {
			curCfgNum := kv.shards[shardId].ConifgNum
			minCfgNum = min(minCfgNum, curCfgNum)
			if curCfgNum+1 > kv.configs[len(kv.configs)-1].Num {
				DPrintf(100, "%v ConfigApplier run for shardId=%v state=%v curCfgNum=%v kv.configs[len(kv.configs)-1].Num=%v",
					kv, shardId, kv.shards[shardId].State, curCfgNum, kv.configs[len(kv.configs)-1].Num)
				continue
			}
			lastCfg := &kv.configs[curCfgNum-kv.configs[0].Num]
			nextCfg := &kv.configs[curCfgNum+1-kv.configs[0].Num]
			DPrintf(100, "%v ConfigApplier run for shardId=%v state=%v lastCfg=%v nextCfg=%v",
				kv, shardId, kv.shards[shardId].State, lastCfg, nextCfg)
			//can apply next config: Serving Idle   cannot: Pulling BePulling GCing
			if kv.shards[shardId].State == Idle {
				if nextCfg.Shards[shardId] != kv.gid {
					argsPut := RequestPutShardArgs{
						NextConfigNum:          nextCfg.Num,
						SrcGid:                 kv.gid,
						DstGid:                 kv.gid,
						ShardId:                shardId,
						ShardKVDB:              nil,
						ShardClientLastRequest: nil,
					}
					kv.rf.Start(argsPut)
				} else {
					argsGet := RequestGetShardArgs{
						NextConfigNum: nextCfg.Num,
						SrcGid:        lastCfg.Shards[shardId],
						DstGid:        kv.gid,
						ShardId:       shardId,
					}
					// go kv.startRequestGetShard(argsGet)
					go func() {
						t := time.Duration(rand.Intn(10))
						time.Sleep(t * time.Millisecond)
						kv.startRequestGetShard(argsGet)
					}()
				}
			}
			if kv.shards[shardId].State == Serving {
				if nextCfg.Shards[shardId] == kv.gid {
					argsPut := RequestPutShardArgs{
						NextConfigNum:          nextCfg.Num,
						SrcGid:                 kv.gid,
						DstGid:                 kv.gid,
						ShardId:                shardId,
						ShardKVDB:              nil,
						ShardClientLastRequest: nil,
					}
					kv.rf.Start(argsPut)
				}
			}
		}
		if minCfgNum-1 >= 0 {
			kv.configs = kv.configs[minCfgNum-1-kv.configs[0].Num:]
		}
		kv.mu.Unlock()
		t := time.Duration(150 + rand.Intn(150))
		time.Sleep(t * time.Millisecond)
	}
}

func (kv *ShardKV) ProcessRequestGetShardL(argsGet RequestGetShardArgs) {
	shardId := argsGet.ShardId
	DPrintf(100, "%v: ProcessRequestGetShardArgsL args=%v kv.shards[%v]:state=%v,confignum=%v",
		kv, argsGet, argsGet.ShardId, kv.shards[shardId].State, kv.shards[shardId].ConifgNum)
	if argsGet.NextConfigNum-1 != kv.shards[shardId].ConifgNum {
		return
	}
	if kv.shards[shardId].State == Serving {
		kv.shards[shardId].State = BePulling
	}
	if kv.shards[shardId].State != BePulling {
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	shardKVDB := make(map[string]string)
	shardClientLastRequest := make(map[int64]Operation)
	deepCopy(&shardKVDB, kv.shards[shardId].KVDB)
	deepCopy(&shardClientLastRequest, kv.shards[argsGet.ShardId].ClientLastOperation)
	argsPut := RequestPutShardArgs{
		NextConfigNum:          argsGet.NextConfigNum,
		SrcGid:                 kv.gid,
		DstGid:                 argsGet.DstGid,
		ShardId:                argsGet.ShardId,
		ShardKVDB:              shardKVDB,
		ShardClientLastRequest: shardClientLastRequest,
	}
	go kv.startRequestPutShardKVDB(argsPut)
}
func (kv *ShardKV) ProcessRequestPutShardL(argsPut RequestPutShardArgs) {
	DPrintf(100, "%v: ProcessRequestPutShardArgsL args=%v kv.shards[%v]:state=%v conifgNum=%v",
		kv, argsPut, argsPut.ShardId, kv.shards[argsPut.ShardId].State, kv.shards[argsPut.ShardId].ConifgNum)
	if argsPut.NextConfigNum <= kv.shards[argsPut.ShardId].ConifgNum {
		return
	}
	shardId := argsPut.ShardId
	shardKVDB := make(map[string]string)
	shardClientLastRequest := make(map[int64]Operation)
	var state ShardState
	if argsPut.SrcGid != kv.gid {
		deepCopy(&shardKVDB, argsPut.ShardKVDB)
		deepCopy(&shardClientLastRequest, argsPut.ShardClientLastRequest)
		if argsPut.SrcGid > 0 {
			state = GCing
		} else {
			state = Serving
		}
	} else {
		shardKVDB = kv.shards[shardId].KVDB
		shardClientLastRequest = kv.shards[shardId].ClientLastOperation
		state = kv.shards[shardId].State
	}
	kv.shards[shardId] = Shard{
		KVDB:                shardKVDB,
		ClientLastOperation: shardClientLastRequest,
		ConifgNum:           argsPut.NextConfigNum,
		State:               state,
	}
}
func (kv *ShardKV) ProcessGCShardL(argsGC RequestGCShardArgs) {
	shardId := argsGC.ShardId
	DPrintf(100, "%v: ProcessGCShardL args=%v kv.shards[%v]: state=%v confignum=%v", kv,
		argsGC, shardId, kv.shards[shardId].State, kv.shards[shardId].ConifgNum)
	if argsGC.CurrentConfigNum-1 == kv.shards[shardId].ConifgNum &&
		kv.shards[shardId].State == BePulling {
		kv.shards[shardId] = Shard{
			KVDB:                nil,
			ClientLastOperation: nil,
			ConifgNum:           argsGC.CurrentConfigNum,
			State:               Idle,
		}
	}
	argsGCAck := RequestGCAckShardArgs(argsGC)
	go kv.startRequestGCAckShard(argsGCAck)
}
func (kv *ShardKV) ProcessGCAckShardL(argsGCAck RequestGCAckShardArgs) {
	shardId := argsGCAck.ShardId
	DPrintf(100, "%v: ProcessGCAckShardL args=%v, kv.shards[%v]: state=%v conifgNum=%v ", kv,
		argsGCAck, shardId, kv.shards[shardId].State, kv.shards[shardId].ConifgNum)
	if argsGCAck.CurrentConfigNum == kv.shards[shardId].ConifgNum &&
		kv.shards[shardId].State == GCing {
		kv.shards[shardId].State = Serving
	}
}
func (kv *ShardKV) ProcessNewConfigL(cfg shardctrler.Config) {
	cntCfg := len(kv.configs)
	DPrintf(100, "%v: ProcessNewConfigL a new config %v my last config num=%v", kv, cfg, kv.configs[len(kv.configs)-1].Num)
	if kv.configs[cntCfg-1].Num+1 == cfg.Num {
		kv.configs = append(kv.configs, cfg)
		DPrintf(100, "%v: add config now configs=%v", kv, kv.configs)
	}
}

func (kv *ShardKV) MonitorNewConfig() {
	ticker := time.NewTicker(150 * time.Millisecond)
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		nextConfigNum := kv.configs[len(kv.configs)-1].Num + 1
		kv.mu.Unlock()
		cfg := kv.sc.Query(nextConfigNum)
		kv.mu.Lock()
		DPrintf(100, "%v: come a config %v my last config num=%v", kv, cfg, kv.configs[len(kv.configs)-1].Num)
		if cfg.Num == kv.configs[len(kv.configs)-1].Num+1 {
			_, _, isLeader := kv.rf.Start(cfg)
			if isLeader {
				DPrintf(100, "%v: start new config=%v", kv, cfg)
			}
		}
		kv.mu.Unlock()
		<-ticker.C
	}
	ticker.Stop()
}
func (kv *ShardKV) MonitorGCShard() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		DPrintf(100, "%v: MonitorGCShard start", kv)
		for shardId := 0; shardId < shardctrler.NShards; shardId++ {
			if kv.shards[shardId].State == GCing {
				formerCfg := kv.configs[kv.shards[shardId].ConifgNum-1-kv.configs[0].Num]
				currentCfg := kv.configs[kv.shards[shardId].ConifgNum-kv.configs[0].Num]
				args := RequestGCShardArgs{
					CurrentConfigNum: kv.shards[shardId].ConifgNum,
					ShardId:          shardId,
					FormerGid:        formerCfg.Shards[shardId],
					CurrentGid:       currentCfg.Shards[shardId],
				}
				// go kv.startRequestGCShard(args)
				go func() {
					t := time.Duration(rand.Intn(10))
					time.Sleep(t * time.Millisecond)
					kv.startRequestGCShard(args)
				}()
			}
		}
		DPrintf(100, "%v: MonitorGCShard end", kv)
		kv.mu.Unlock()
		t := time.Duration(100 + rand.Intn(100))
		time.Sleep(t * time.Millisecond)
	}
}

//	func (kv *ShardKV) getConfigL(num int) shardctrler.Config {
//		if num >= len(kv.configs) {
//			return kv.sc.Query(num)
//		}
//		return kv.configs[num]
//	}
func (kv *ShardKV) Gid2ServersL(gid int) []string {
	lastCfg := &kv.configs[len(kv.configs)-1]
	if servers, exist := lastCfg.Groups[gid]; exist {
		return servers
	}
	if servers, exist := lastCfg.GroupsDeleted[gid]; exist {
		return servers
	}
	cfg := kv.sc.Query(-1)
	DPrintf(800, "%v: query", kv)
	if servers, exist := cfg.Groups[gid]; exist {
		return servers
	}
	if servers, exist := cfg.GroupsDeleted[gid]; exist {
		return servers
	}
	errInfo := fmt.Sprintf("can find servers when gid=%v", gid)
	panic(errInfo)
}
