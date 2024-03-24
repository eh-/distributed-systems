package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OType int

const (
	JoinOp OType = iota
	LeaveOp
	MoveOp
	QueryOp

	ExecuteTimeout = time.Millisecond * 1000
)

type Op struct {
	// Your data here.
	OpType    OType
	ClientId  int64
	CommandId int64
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}

type OpResult struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type LastResult struct {
	CommandId  int64
	LastResult OpResult
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	notifyChs      map[int]chan Op
	userLastResult map[int64]LastResult
	configs        []Config // indexed by config num
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType:    JoinOp,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	result := sc.handleOperation(op)
	reply.WrongLeader, reply.Err = result.WrongLeader, result.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType:    LeaveOp,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	result := sc.handleOperation(op)
	reply.WrongLeader, reply.Err = result.WrongLeader, result.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType:    MoveOp,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	result := sc.handleOperation(op)
	reply.WrongLeader, reply.Err = result.WrongLeader, result.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType:    QueryOp,
		Num:       args.Num,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	result := sc.handleOperation(op)
	reply.WrongLeader, reply.Err, reply.Config = result.WrongLeader, result.Err, result.Config
}

func (sc *ShardCtrler) handleOperation(operation Op) OpResult {
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		return OpResult{
			WrongLeader: true,
		}
	}

	sc.mu.Lock()
	if lastResult, exist := sc.userLastResult[operation.ClientId]; exist && lastResult.CommandId == operation.CommandId {
		result := OpResult{
			WrongLeader: lastResult.LastResult.WrongLeader,
			Err:         lastResult.LastResult.Err,
			Config:      lastResult.LastResult.Config,
		}
		sc.mu.Unlock()
		return result
	} else if exist && lastResult.CommandId > operation.CommandId {
		sc.mu.Unlock()
		return OpResult{
			Err: ErrTimeout,
		}
	}
	sc.mu.Unlock()

	startIndex, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		return OpResult{
			WrongLeader: true,
		}
	}
	sc.mu.Lock()
	ch := make(chan Op)
	sc.notifyChs[startIndex] = ch
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChs, startIndex)
		sc.mu.Unlock()
	}()

	select {
	case <-time.After(ExecuteTimeout):
		return OpResult{
			Err: ErrTimeout,
		}
	case commitOperation := <-ch:
		if commitOperation.ClientId != operation.ClientId || commitOperation.CommandId != operation.CommandId {
			return OpResult{
				WrongLeader: true,
			}
		}

		sc.mu.Lock()
		lastResult, exist := sc.userLastResult[commitOperation.ClientId]
		if !exist || lastResult.CommandId != commitOperation.CommandId {
			sc.mu.Unlock()
			return OpResult{
				Err: ErrTimeout,
			}
		}
		sc.mu.Unlock()
		return lastResult.LastResult
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for {
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid {
			operation := applyMsg.Command.(Op)
			index := applyMsg.CommandIndex
			sc.mu.Lock()
			lastResult, exist := sc.userLastResult[operation.ClientId]
			if !exist || lastResult.CommandId < operation.CommandId {
				result := sc.databaseExecute(operation)
				sc.userLastResult[operation.ClientId] = LastResult{
					CommandId:  operation.CommandId,
					LastResult: result,
				}
			}
			ch, exist := sc.notifyChs[index]

			sc.mu.Unlock()

			if !exist {
				continue
			}

			ch <- operation
		} else {
			panic("Unexpected message in apply channel")
		}
	}
}

func (sc *ShardCtrler) databaseExecute(operation Op) OpResult {
	switch operation.OpType {
	case JoinOp:
		lastConfig := sc.configs[len(sc.configs)-1]
		newShards := lastConfig.Shards
		newGroups := make(map[int][]string)
		for k, v := range lastConfig.Groups {
			newGroups[k] = v
		}

		for gid, servers := range operation.Servers {
			if _, gidExist := newGroups[gid]; gidExist {
				return OpResult{
					Err: ErrGroupAlreadyExist,
				}
			}
			newGroups[gid] = servers
		}
		moveShards := []int{}
		shardCount := make(map[int]int)
		newGroupLength := len(newGroups)
		for i, extra := 0, NShards%newGroupLength; i < NShards; i++ {
			maxSize := NShards / newGroupLength
			if extra > 0 {
				maxSize++
			}
			if _, exist := newGroups[lastConfig.Shards[i]]; !exist {
				moveShards = append(moveShards, i)
				continue
			}
			if shardCount[lastConfig.Shards[i]]+1 > maxSize {
				moveShards = append(moveShards, i)
			} else {
				shardCount[lastConfig.Shards[i]]++
				if shardCount[lastConfig.Shards[i]] == maxSize && extra > 0 {
					extra--
				}
			}
		}

		gs := []GroupShards{}
		for gid := range newGroups {
			if count, exist := shardCount[gid]; exist {
				gs = append(gs, GroupShards{gid, count})
			} else {
				gs = append(gs, GroupShards{gid, 0})
			}
		}
		sort.Sort(LargerShardCount(gs))

		for i, j, extra := 0, 0, NShards%newGroupLength; i < len(gs) && j < len(moveShards); i++ {
			currGroupCount := NShards / newGroupLength
			if extra > 0 {
				currGroupCount++
				extra--
			}
			for gs[i].shardCount < currGroupCount && j < len(moveShards) {
				gs[i].shardCount++
				newShards[moveShards[j]] = gs[i].gid
				j++
			}
		}

		newConfig := Config{
			Num:    len(sc.configs),
			Shards: newShards,
			Groups: newGroups,
		}

		sc.configs = append(sc.configs, newConfig)
		return OpResult{
			Err: OK,
		}

	case LeaveOp:
		lastConfig := sc.configs[len(sc.configs)-1]
		newShards := lastConfig.Shards
		newGroups := make(map[int][]string)
		for k, v := range lastConfig.Groups {
			newGroups[k] = v
		}
		for _, gid := range operation.GIDs {
			if _, gidExist := newGroups[gid]; !gidExist {
				return OpResult{
					Err: ErrNoGroup,
				}
			}
			delete(newGroups, gid)
		}

		if len(newGroups) == 0 {
			newConfig := Config{
				Num:    len(sc.configs),
				Shards: [NShards]int{},
				Groups: newGroups,
			}
			sc.configs = append(sc.configs, newConfig)
			return OpResult{
				Err: OK,
			}
		}

		moveShards := []int{}
		shardCount := make(map[int]int)
		for shardIndex, groupId := range lastConfig.Shards {
			if _, gidExist := newGroups[groupId]; !gidExist {
				moveShards = append(moveShards, shardIndex)
			} else {
				shardCount[groupId]++
			}
		}

		gs := []GroupShards{}
		for gid := range newGroups {
			if count, exist := shardCount[gid]; exist {
				gs = append(gs, GroupShards{gid, count})
			} else {
				gs = append(gs, GroupShards{gid, 0})
			}
		}
		sort.Sort(LargerShardCount(gs))

		newGroupCount := len(newGroups)
		for i, j, extraGroups := 0, 0, NShards%newGroupCount; i < len(gs) && j < len(moveShards); i++ {
			currGroupCount := NShards / newGroupCount
			if extraGroups > 0 {
				currGroupCount++
				extraGroups--
			}
			for gs[i].shardCount < currGroupCount && j < len(moveShards) {
				gs[i].shardCount++
				newShards[moveShards[j]] = gs[i].gid
				j++
			}
		}

		newConfig := Config{
			Num:    len(sc.configs),
			Shards: newShards,
			Groups: newGroups,
		}

		sc.configs = append(sc.configs, newConfig)
		return OpResult{
			Err: OK,
		}

	case MoveOp:
		lastConfig := sc.configs[len(sc.configs)-1]
		if _, exist := lastConfig.Groups[operation.GID]; !exist {
			return OpResult{
				Err: ErrNoGroup,
			}
		}
		newConfig := Config{
			Num:    len(sc.configs),
			Shards: lastConfig.Shards,
			Groups: lastConfig.Groups,
		}
		newConfig.Shards[operation.Shard] = operation.GID
		sc.configs = append(sc.configs, newConfig)
		return OpResult{
			Err: OK,
		}

	case QueryOp:
		result := OpResult{
			Err: OK,
		}
		if operation.Num == -1 || operation.Num >= len(sc.configs) {
			result.Config = sc.configs[len(sc.configs)-1]
		} else {
			result.Config = sc.configs[operation.Num]
		}
		return result
	}
	return OpResult{
		Err: "NoOp",
	}
}

type GroupShards struct {
	gid        int
	shardCount int
}

type LargerShardCount []GroupShards

func (gs LargerShardCount) Len() int {
	return len(gs)
}

func (gs LargerShardCount) Swap(i, j int) {
	gs[i], gs[j] = gs[j], gs[i]
}

func (gs LargerShardCount) Less(i, j int) bool {
	return gs[i].shardCount > gs[j].shardCount || (gs[i].shardCount == gs[j].shardCount && gs[i].gid > gs[j].gid)
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

	sc.notifyChs = make(map[int]chan Op)
	sc.userLastResult = make(map[int64]LastResult)

	go sc.applier()

	return sc
}
