package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type OType int

const (
	GetOp OType = iota
	PutOp
	AppendOp
	UpdateConfig
	AddShard
	RemoveShard

	ExecuteTimeout      = time.Millisecond * 1000
	ConfigCheckInterval = time.Millisecond * 50
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType         OType
	Key            string
	Val            string
	ClientId       int64
	CommandId      int64
	Config         shardctrler.Config
	ShardNum       int
	Shard          Shard
	UserLastResult map[int64]OpResult
}

type OpResult struct {
	ClientId  int64
	CommandId int64
	Err       Err
	Value     string
}

// type LastResult struct {
// 	CommandId  int64
// 	LastResult OpResult
// }

type Shard struct {
	ConfigNum int
	Shard     map[string]string
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
	lastApplied  int
	dead         int32

	// Your definitions here.
	scClerk        *shardctrler.Clerk
	lastConfig     shardctrler.Config
	currentConfig  shardctrler.Config
	stateMachine   [shardctrler.NShards]Shard
	notifyChs      map[int]chan OpResult
	userLastResult map[int64]OpResult

	persister *raft.Persister
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardNum := key2shard(args.Key)
	kv.mu.Lock()
	if lastResult, exist := kv.userLastResult[args.ClientId]; exist && lastResult.CommandId == args.CommandId {
		reply.Err, reply.Value = lastResult.Err, lastResult.Value
		kv.mu.Unlock()
		return
	}
	if kv.currentConfig.Shards[shardNum] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.stateMachine[shardNum].Shard == nil {
		reply.Err = ErrShardMissing
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    GetOp,
		Key:       args.Key,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	result := kv.addRaftLog(op)
	reply.Err, reply.Value = result.Err, result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardNum := key2shard(args.Key)
	kv.mu.Lock()
	if lastResult, exist := kv.userLastResult[args.ClientId]; exist && lastResult.CommandId == args.CommandId {
		reply.Err = lastResult.Err
		kv.mu.Unlock()
		return
	}
	if kv.currentConfig.Shards[shardNum] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.stateMachine[shardNum].Shard == nil {
		reply.Err = ErrShardMissing
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Key:       args.Key,
		Val:       args.Value,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	if args.Op == "Put" {
		op.OpType = PutOp
	} else {
		op.OpType = AppendOp
	}
	result := kv.addRaftLog(op)
	reply.Err = result.Err
}

func (kv *ShardKV) addRaftLog(operation Op) OpResult {
	index, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		return OpResult{
			Err: ErrWrongLeader,
		}
	}
	kv.mu.Lock()
	ch := make(chan OpResult)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(ExecuteTimeout):
		return OpResult{
			Err: ErrTimeout,
		}
	case commitResult := <-ch:
		if commitResult.ClientId != operation.ClientId || commitResult.CommandId != operation.CommandId {
			return OpResult{
				Err: ErrWrongLeader,
			}
		}
		return commitResult
	}
}

type SendShardArgs struct {
	ShardNum       int
	Shard          Shard
	UserLastResult map[int64]OpResult
	ClientId       int64
	CommandId      int64
}

type SendShardReply struct {
	Err Err
}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	kv.mu.Lock()
	if kv.stateMachine[args.ShardNum].ConfigNum == args.Shard.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	operation := Op{
		OpType:         AddShard,
		ClientId:       args.ClientId,
		CommandId:      args.CommandId,
		ShardNum:       args.ShardNum,
		Shard:          args.Shard,
		UserLastResult: args.UserLastResult,
	}
	result := kv.addRaftLog(operation)
	reply.Err = result.Err
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) configDetector() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(ConfigCheckInterval)
			continue
		}
		kv.mu.Lock()
		if !kv.allSent() {
			userLastResult := make(map[int64]OpResult)
			for clientId, lastResult := range kv.userLastResult {
				userLastResult[clientId] = lastResult
			}

			for shardNum, gid := range kv.lastConfig.Shards {
				if gid == kv.gid && kv.currentConfig.Shards[shardNum] != kv.gid && kv.stateMachine[shardNum].ConfigNum < kv.currentConfig.Num {
					shard := kv.cloneShard(kv.currentConfig.Num, kv.stateMachine[shardNum].Shard)
					args := SendShardArgs{
						UserLastResult: userLastResult,
						ShardNum:       shardNum,
						Shard:          shard,
						ClientId:       int64(gid),
						CommandId:      int64(kv.currentConfig.Num),
					}

					sendGroupNames := kv.currentConfig.Groups[kv.currentConfig.Shards[shardNum]]
					go func(serverNames []string, args SendShardArgs) {
						index := 0
						for {
							reply := SendShardReply{}
							if kv.make_end(serverNames[index]).Call("ShardKV.SendShard", &args, &reply) && reply.Err == OK {
								kv.mu.Lock()
								operation := Op{
									OpType:    RemoveShard,
									ClientId:  int64(kv.gid),
									CommandId: int64(kv.currentConfig.Num),
									ShardNum:  args.ShardNum,
								}
								kv.mu.Unlock()
								kv.addRaftLog(operation)
								break
							}
							index = (index + 1) % len(serverNames)
							if index == 0 {
								time.Sleep(ConfigCheckInterval)
							}
						}
					}(sendGroupNames, args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(ConfigCheckInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(ConfigCheckInterval)
			continue
		}
		currentConfig := kv.currentConfig
		kv.mu.Unlock()
		newConfig := kv.scClerk.Query(currentConfig.Num + 1)
		if newConfig.Num != currentConfig.Num+1 {
			time.Sleep(ConfigCheckInterval)
			continue
		}
		operation := Op{
			OpType:    UpdateConfig,
			ClientId:  int64(kv.gid),
			CommandId: int64(newConfig.Num),
			Config:    newConfig,
		}
		kv.addRaftLog(operation)
	}
}

func (kv *ShardKV) cloneShard(configNum int, kvShard map[string]string) Shard {
	clone := Shard{
		ConfigNum: configNum,
		Shard:     make(map[string]string),
	}
	for key, value := range kvShard {
		clone.Shard[key] = value
	}
	return clone
}

func (kv *ShardKV) allSent() bool {
	for shardNum, gid := range kv.lastConfig.Shards {
		if gid == kv.gid && kv.currentConfig.Shards[shardNum] != kv.gid && kv.stateMachine[shardNum].ConfigNum < kv.currentConfig.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shardNum, gid := range kv.lastConfig.Shards {
		if gid != kv.gid && kv.currentConfig.Shards[shardNum] == kv.gid && kv.stateMachine[shardNum].ConfigNum < kv.currentConfig.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				operation := applyMsg.Command.(Op)
				result := OpResult{
					ClientId:  operation.ClientId,
					CommandId: operation.CommandId,
				}
				index := applyMsg.CommandIndex
				kv.mu.Lock()
				if applyMsg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				if operation.OpType == GetOp || operation.OpType == AppendOp || operation.OpType == PutOp {
					shardNum := key2shard(operation.Key)
					if kv.currentConfig.Shards[shardNum] != kv.gid {
						result.Err = ErrWrongGroup
					} else if kv.stateMachine[shardNum].Shard == nil {
						result.Err = ErrShardMissing
					} else {
						lastResult, exist := kv.userLastResult[operation.ClientId]
						if !exist || lastResult.CommandId < operation.CommandId {
							result = kv.databaseExecute(operation)
							kv.userLastResult[operation.ClientId] = result
						}
					}

				} else {
					switch operation.OpType {
					case UpdateConfig:
						kv.updateConfig(operation)
						result.Err = OK
					case AddShard:
						if kv.currentConfig.Num < int(operation.CommandId) {
							// config not arrived
							result.Err = ErrEarlySendShard
						} else {
							kv.addShard(operation)
							result.Err = OK
						}
					case RemoveShard:
						kv.removeShard(operation)
						result.Err = OK
					}
				}

				if kv.needSnapshot() {
					kv.takeSnapshot(index)
				}
				kv.lastApplied = applyMsg.CommandIndex
				ch, exist := kv.notifyChs[index]
				kv.mu.Unlock()
				if exist {
					ch <- result
				}
			} else if applyMsg.SnapshotValid {
				kv.mu.Lock()
				kv.readSnapshot(applyMsg.Snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex
				kv.mu.Unlock()
			} else {
				panic("unexpected message in applyCh")
			}
		}
	}
}

func (kv *ShardKV) databaseExecute(op Op) (res OpResult) {
	shardNum := key2shard(op.Key)
	result := OpResult{
		ClientId:  op.ClientId,
		CommandId: op.CommandId,
	}

	switch op.OpType {
	case GetOp:
		if value, ok := kv.stateMachine[shardNum].Shard[op.Key]; ok {
			result.Err, result.Value = OK, value
			return result
		}
		result.Err, result.Value = ErrNoKey, ""
		return result
	case PutOp:
		kv.stateMachine[shardNum].Shard[op.Key] = op.Val
		result.Err = OK
		return result
	case AppendOp:
		kv.stateMachine[shardNum].Shard[op.Key] += op.Val
		result.Err = OK
		return result
	}
	return
}

func (kv *ShardKV) updateConfig(operation Op) {
	currentConfig := kv.currentConfig
	nextConfig := operation.Config
	if currentConfig.Num >= nextConfig.Num {
		return
	}
	for shardNum, gid := range nextConfig.Shards {
		if gid == kv.gid && currentConfig.Shards[shardNum] == 0 {
			kv.stateMachine[shardNum].Shard = make(map[string]string)
			kv.stateMachine[shardNum].ConfigNum = nextConfig.Num
		}
	}
	kv.lastConfig, kv.currentConfig = currentConfig, nextConfig
}

func (kv *ShardKV) addShard(operation Op) {
	if kv.stateMachine[operation.ShardNum].Shard != nil || operation.Shard.ConfigNum < kv.currentConfig.Num {
		return
	}

	kv.stateMachine[operation.ShardNum] = kv.cloneShard(operation.Shard.ConfigNum, operation.Shard.Shard)

	for clientId, lastResult := range operation.UserLastResult {
		if result, exist := kv.userLastResult[clientId]; !exist || result.CommandId < lastResult.CommandId {
			kv.userLastResult[clientId] = lastResult
		}
	}
}

func (kv *ShardKV) removeShard(operation Op) {
	if operation.CommandId < int64(kv.currentConfig.Num) {
		return
	}
	kv.stateMachine[operation.ShardNum].Shard = nil
	kv.stateMachine[operation.ShardNum].ConfigNum = int(operation.CommandId)
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	currentLength := kv.persister.RaftStateSize()
	return currentLength/9 >= kv.maxraftstate/10
}

func (kv *ShardKV) takeSnapshot(lastAppliedIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.userLastResult)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	snapshot := w.Bytes()
	go kv.rf.Snapshot(lastAppliedIndex, snapshot)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine [shardctrler.NShards]Shard
	var userLastResult map[int64]OpResult
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	if d.Decode(&stateMachine) != nil || d.Decode(&userLastResult) != nil || d.Decode(&lastConfig) != nil || d.Decode(&currentConfig) != nil {
		panic("failed to decode snapshot data")
	}
	kv.stateMachine = stateMachine
	kv.userLastResult = userLastResult
	kv.lastConfig = lastConfig
	kv.currentConfig = currentConfig
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.scClerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.lastApplied = 0

	for i := range kv.stateMachine {
		kv.stateMachine[i].ConfigNum = 0
		kv.stateMachine[i].Shard = nil
	}
	kv.userLastResult = make(map[int64]OpResult)
	defaultConfig := shardctrler.Config{
		Num:    0,
		Groups: make(map[int][]string),
	}
	kv.currentConfig, kv.lastConfig = defaultConfig, defaultConfig
	kv.notifyChs = make(map[int]chan OpResult)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.configDetector()

	return kv
}
