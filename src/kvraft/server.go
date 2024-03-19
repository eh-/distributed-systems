package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

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

// type KVStateMachine interface {
// 	Get(key string) (string, Err)
// 	Put(key, value string) Err
// 	Append(kev, value string) Err
// }

// type MemoryKV struct {
// 	KV map[string]string
// }

// func NewMemoryKV() *MemoryKV {
// 	return &MemoryKV{make(map[string]string)}
// }

// func (mKV *MemoryKV) Get(key string) (string, Err) {
// 	if value, ok := mKV.KV[key]; ok {
// 		return value, OK
// 	}
// 	return "", ErrNoKey
// }

// func (mKV *MemoryKV) Put(key, value string) Err {
// 	mKV.KV[key] = value
// 	return OK
// }

// func (mKV *MemoryKV) Append(key, value string) Err {
// 	mKV.KV[key] += value
// 	return OK
// }

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
	stateMachine map[string]string
	notifyChs    map[int64]map[int64]chan *OpResult
	//commandRes map[int64]map[int64] *result
}

func (kv *KVServer) addChannel(clientId int64, commandId int64, ch chan *OpResult) {
	if kv.notifyChs[clientId] == nil {
		kv.notifyChs[clientId] = make(map[int64]chan *OpResult)
	}
	kv.notifyChs[clientId][commandId] = ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    OpGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan *OpResult)
	kv.addChannel(args.ClientId, args.CommandId, ch)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err, reply.Value = result.Err, result.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Val:       args.Value,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	if args.Op == "Put" {
		op.OpType = OpPut
	} else {
		op.OpType = OpAppend
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan *OpResult)
	kv.addChannel(args.ClientId, args.CommandId, ch)
	kv.mu.Unlock()

	result := <-ch
	reply.Err = result.Err
}

func (kv *KVServer) databaseExecute(op *Op) (res OpResult) {
	switch op.OpType {
	case OpGet:
		if value, ok := kv.stateMachine[op.Key]; ok {
			return OpResult{
				Value: value,
				Err:   OK,
			}
		}
		return OpResult{
			Value: "",
			Err:   ErrNoKey,
		}
	case OpPut:
		kv.stateMachine[op.Key] = op.Val
		return OpResult{
			Err: OK,
		}
	case OpAppend:
		kv.stateMachine[op.Key] += op.Val
		return OpResult{
			Err: OK,
		}
	}
	return
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
				kv.mu.Lock()
				operation := applyMsg.Command.(Op)
				result := kv.databaseExecute(&operation)
				ch := kv.notifyChs[operation.ClientId][operation.CommandId]
				ch <- &result
				kv.mu.Unlock()
			} else {
				panic("unexpected message in applyCh")
			}
		}
	}
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
	kv.notifyChs = make(map[int64]map[int64]chan *OpResult)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
