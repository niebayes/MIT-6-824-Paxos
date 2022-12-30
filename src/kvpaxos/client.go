package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
)

// backoff params.
const backoffFactor = 2
const initWaitTime = 25 * time.Millisecond
const maxWaitTime = 1 * time.Second

type Clerk struct {
	// host addresses of kv servers.
	servers []string
	// the id (index into servers array) of the server the clerk successfully sent request to.
	lastAliveServerId int
	// the id of this clerk.
	// this id is generated from nrand on init.
	// we assume the clerk ids of different clerks will not collide so that the paxos servers could
	// differentiate between clerks.
	// this and the subsequent nextOpId are used to detect duplicate requests and
	// to ensure the at-most-once semantics.
	clerkId int64
	// the next operation id to allocate.
	nextOpId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastAliveServerId = 0
	ck.clerkId = nrand()
	ck.nextOpId = 0
	return ck
}

func (ck *Clerk) allocateOpId() int {
	opId := ck.nextOpId
	ck.nextOpId++
	return opId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), Key: key}
	reply := &GetReply{}

	lastWaitTime := initWaitTime

	for {
		// at first, try to send the request to the server the last time successfully sent the request to.
		// if it fails, try other servers in a row.
		aliveServerId := ck.lastAliveServerId
		for i := range ck.servers {
			serverId := (aliveServerId + i) % len(ck.servers)
			if !call(ck.servers[serverId], "KVPaxos.Get", args, reply) {
				printf("C%v failed to contact with S%v", ck.clerkId, serverId)
				continue
			}
			ck.lastAliveServerId = serverId

			printf("C%v receives reply %v from S%v", ck.clerkId, reply.Err, serverId)

			if reply.Err == OK || reply.Err == ErrNoKey {
				return reply.Value
			}
		}

		// wait a while and retry.
		waitTime := backoffFactor * lastWaitTime
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
		time.Sleep(waitTime)
		lastWaitTime = waitTime
	}
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), OpType: op, Key: key, Value: value}
	reply := &PutAppendReply{}

	lastWaitTime := initWaitTime

	for {
		// at first, try to send the request to the server the last time successfully sent the request to.
		// if it fails, try other servers in a row.
		aliveServerId := ck.lastAliveServerId
		for i := range ck.servers {
			serverId := (aliveServerId + i) % len(ck.servers)
			if !call(ck.servers[serverId], "KVPaxos.PutAppend", args, reply) {
				printf("C%v failed to contact with S%v", ck.clerkId, serverId)
				continue
			}
			ck.lastAliveServerId = serverId

			printf("C%v receives reply %v from S%v", ck.clerkId, reply.Err, serverId)

			if reply.Err == OK {
				return
			}
		}

		// wait a while and retry.
		waitTime := backoffFactor * lastWaitTime
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
		time.Sleep(waitTime)
		lastWaitTime = waitTime
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
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
