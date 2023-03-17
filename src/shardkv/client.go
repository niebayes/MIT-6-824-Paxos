package shardkv

import "6.824/src/shardmaster"
import "net/rpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	clerkId  int64
	nextOpId int
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

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), Key: key}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]

		if ok {
			for _, srv := range servers {
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

		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), OpType: op, Key: key, Value: value}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]

		if ok {
			for _, srv := range servers {
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

		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

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
