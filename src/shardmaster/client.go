package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"sync"
	"time"
)

type Clerk struct {
	servers  []string // shardmaster replicas
	clerkId  int64    // the id of this clerk.
	nextOpId int      // the next op id to allocate.
	mu       sync.Mutex
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.nextOpId = 0
	ck.mu = sync.Mutex{}
	return ck
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) allocateOpId() int {
	opId := ck.nextOpId
	ck.nextOpId++
	return opId
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &QueryArgs{Num: num, ClerkId: ck.clerkId, OpId: ck.allocateOpId()}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := call(srv, "ShardMaster.Query", args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(gid int64, servers []string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &JoinArgs{GID: gid, Servers: servers, ClerkId: ck.clerkId, OpId: ck.allocateOpId()}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := call(srv, "ShardMaster.Join", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gid int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &LeaveArgs{GID: gid, ClerkId: ck.clerkId, OpId: ck.allocateOpId()}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := call(srv, "ShardMaster.Leave", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &MoveArgs{Shard: shard, GID: gid, ClerkId: ck.clerkId, OpId: ck.allocateOpId()}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := call(srv, "ShardMaster.Move", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
