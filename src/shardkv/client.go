package shardkv

import "6.824/src/shardmaster"
import "net/rpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	sm       *shardmaster.Clerk // used to contact with the shardmaster service.
	config   shardmaster.Config // clerk's current config.
	clerkId  int64              // the unique id of this clerk.
	nextOpId int                // the next op id to allocate.
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	ck.config = shardmaster.Config{Num: 0, Groups: make(map[int64][]string)}
	ck.clerkId = nrand()
	ck.nextOpId = 0
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

// which shard is a key in?
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), Key: key}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				println("C%v sends Get (Id=%v K=%v) to G%v S%v", args.ClerkId, args.OpId, args.Key, gid, srv)

				var reply GetReply
				ok := call(srv, "ShardKV.Get", args, &reply)
				if ok && reply.Err == OK {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask the shardmaster for the latest configuration.
		//
		// note: it's possible that the config change is too often so that
		// the server's config is way too lag-behind than the client's config.
		// since the config change and shard migration may be time-consuming,
		// the client might have to wait a long time to wait the server changes
		// to a config wherein the server is eligible to serve the key.
		//
		// if a 30s timeout is set on the tests, there's 1/500 fail rate.
		//
		// hence, instead of always quering the latest config, we choose to
		// query the next config when notified ErrWrongGroup.
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

// send a Put or Append request.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), OpType: op, Key: key, Value: value}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				println("C%v sends PutAppend (Id=%v T=%v K=%v V=%v) to G%v S%v", args.ClerkId, args.OpId, args.OpType, args.Key, args.Value, gid, srv)

				var reply PutAppendReply
				ok := call(srv, "ShardKV.PutAppend", args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask the shardmaster for the latest configuration.
		ck.config = ck.sm.Query(ck.config.Num + 1)
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
	return err == nil
}
