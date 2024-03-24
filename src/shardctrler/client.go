package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	leaderId  int
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// Your code here.
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 1,
	}
}

func (ck *Clerk) getUniqueCommandId() int64 {
	commandId := ck.commandId
	ck.commandId++
	return commandId
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:       num,
		ClientId:  ck.clientId,
		CommandId: ck.getUniqueCommandId(),
	}
	// Your code here.
	for {
		reply := &QueryReply{}
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply) || reply.WrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		CommandId: ck.getUniqueCommandId(),
	}
	// Your code here.

	for {
		reply := &JoinReply{}
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply) || reply.WrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.clientId,
		CommandId: ck.getUniqueCommandId(),
	}
	// Your code here.

	for {
		reply := &LeaveReply{}
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply) || reply.WrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		CommandId: ck.getUniqueCommandId(),
	}
	// Your code here.

	for {
		reply := &MoveReply{}
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply) || reply.WrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return
	}
}
