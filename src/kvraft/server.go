package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OType int

const (
	OpGet OType = iota
	OpPut
	OpAppend

	ExecuteTimeout = time.Millisecond * 1000
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OType
	Key       string
	Val       string
	ClientId  int64
	CommandId int64
}

type OpResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	//lastApplied int

	// Your definitions here.
	//stateMachine KVStateMachine
	stateMachine  map[string]string
	notifyChs     map[int]chan Op
	resultHistory map[int64]map[int64]*OpResult
}

func (kv *KVServer) addResult(clientId, commandId int64, result *OpResult) {
	if kv.resultHistory[clientId] == nil {
		kv.resultHistory[clientId] = make(map[int64]*OpResult)
	}
	kv.resultHistory[clientId][commandId] = result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if result, exist := kv.resultHistory[args.ClientId][args.CommandId]; exist {
		reply.Err, reply.Value = result.Err, result.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    OpGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	result := kv.handleOperation(op)
	reply.Err, reply.Value = result.Err, result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if result, exist := kv.resultHistory[args.ClientId][args.CommandId]; exist {
		reply.Err = result.Err
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
		op.OpType = OpPut
	} else {
		op.OpType = OpAppend
	}
	result := kv.handleOperation(op)
	reply.Err = result.Err
}

func (kv *KVServer) handleOperation(operation Op) *OpResult {
	startIndex, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		return &OpResult{
			Err: ErrWrongLeader,
		}
	}
	kv.mu.Lock()
	ch := make(chan Op)
	kv.notifyChs[startIndex] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChs, startIndex)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(ExecuteTimeout):
		return &OpResult{
			Err: ErrTimeout,
		}
	case commitOperation := <-ch:
		if commitOperation.ClientId != operation.ClientId || commitOperation.CommandId != operation.CommandId {
			return &OpResult{
				Err: ErrWrongLeader,
			}
		}

		kv.mu.Lock()
		result := kv.resultHistory[commitOperation.ClientId][commitOperation.CommandId]
		kv.mu.Unlock()
		return result
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {

				operation := applyMsg.Command.(Op)
				index := applyMsg.CommandIndex
				kv.mu.Lock()
				_, exist := kv.resultHistory[operation.ClientId][operation.CommandId]
				if !exist {
					result := kv.databaseExecute(&operation)
					kv.addResult(operation.ClientId, operation.CommandId, result)
				}

				ch, exist := kv.notifyChs[index]
				kv.mu.Unlock()

				if !exist {
					continue
				}

				ch <- operation
			} else {
				panic("unexpected message in applyCh")
			}
		}
	}
}

func (kv *KVServer) databaseExecute(op *Op) (res *OpResult) {
	switch op.OpType {
	case OpGet:
		if value, ok := kv.stateMachine[op.Key]; ok {
			return &OpResult{
				Err:   OK,
				Value: value,
			}
		}
		return &OpResult{
			Err:   ErrNoKey,
			Value: "",
		}
	case OpPut:
		kv.stateMachine[op.Key] = op.Val
		return &OpResult{
			Err: OK,
		}
	case OpAppend:
		value, ok := kv.stateMachine[op.Key]
		if ok {
			kv.stateMachine[op.Key] = value + op.Val
		} else {
			kv.stateMachine[op.Key] = op.Val
		}
		return &OpResult{
			Err: OK,
		}
	}
	return
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = make(map[string]string)
	kv.notifyChs = make(map[int]chan Op)
	kv.resultHistory = make(map[int64]map[int64]*OpResult)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
