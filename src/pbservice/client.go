package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"

	"6.824/src/viewservice"
)

type Clerk struct {
	// view service clerk through which this clerk communicates with the view service to
	// fetch the latest primary in the pb server cluster.
	vs *viewservice.Clerk
	// cached primary address.
	// the clerk will try to fetch the latest primary from the view service when it cannot contact with
	// the current primary.
	primary string
	// the id of this clerk.
	// this id is generated from nrand on init.
	// we assume the clerk ids of different clerks will not collide so that the pb servers could
	// differentiate between clerks.
	// this and the subsequent nextOpId are used to detect duplicate requests and
	// to ensure the at-most-once semantics.
	clerkId int64
	// the next operation id to allocate.
	nextOpId uint
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) allocateOpId() uint {
	opId := ck.nextOpId
	ck.nextOpId += 1
	return opId
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	ck.primary = ""
	ck.clerkId = nrand()
	ck.nextOpId = 0
	return ck
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

// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	// try to fetch the latest primary when there's no primary.
	// this could only happen when this clerk has just restarted or just joined the cluster.
	for ck.primary == "" {
		ck.primary = ck.vs.Primary()
		time.Sleep(viewservice.PingInterval)
	}

	opId := ck.allocateOpId()
	args := &GetArgs{Me: ck.clerkId, OpId: opId, Key: key, Primary: ck.primary}
	reply := &GetReply{}

	for {
		maybePrintf("C%v sending Get(%v, %v)", ck.clerkId, key, opId)
		// keep args' Primary field up-to-date.
		args.Primary = ck.primary
		for !call(ck.primary, "PBServer.Get", args, reply) {
			// failed to contact with the primary, try to fetch the latest primary.
			maybePrintf("C%v failed to contact with primary S%v", ck.clerkId, ck.primary)
			ck.primary = ck.vs.Primary()
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			return reply.Value
		}
		// all other errors will not stop the operation.
		if reply.Err == ErrWrongServer {
			ck.primary = ck.vs.Primary()
		}
	}
}

// send a Put or Append RPC
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// try to fetch the latest primary when there's no primary.
	// this could only happen when this clerk has just restarted or just joined the cluster.
	for ck.primary == "" {
		ck.primary = ck.vs.Primary()
		time.Sleep(viewservice.PingInterval)
	}

	opId := ck.allocateOpId()
	args := &PutAppendArgs{Me: ck.clerkId, OpId: opId, Key: key, Value: value, Op: op, Primary: ck.primary}
	reply := &PutAppendReply{}

	for {
		maybePrintf("C%v sending PutAppend(%v, %v, %v)", ck.clerkId, key, value, opId)
		// keep args' Primary field up-to-date.
		args.Primary = ck.primary
		for !call(ck.primary, "PBServer.PutAppend", args, reply) {
			// failed to contact with the primary, try to fetch the latest primary.
			maybePrintf("C%v failed to contact with primary S%v", ck.clerkId, ck.primary)
			ck.primary = ck.vs.Primary()
		}
		if reply.Err == OK {
			break
		}
		// all other errors will not stop the operation.
		if reply.Err == ErrWrongServer {
			ck.primary = ck.vs.Primary()
		}
	}
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// tell the primary to append to key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
