package shardmaster

//
// Shard master: assigns shards to replication groups.
//
// RPC interface:
// Join(gid, servers) -- add a replica groups (gid -> server-list mapping).
// Leave(gid) -- delete a replica groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const NShards = 10

type Config struct {
	Num    int                // config number
	Shards [NShards]int64     // shard -> gid
	Groups map[int64][]string // gid -> servers[]
}

func (cfg *Config) clonedWithIncNum() Config {
	newConfig := Config{}

	newConfig.Num = cfg.Num

	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = cfg.Shards[i]
	}

	newConfig.Groups = make(map[int64][]string)
	for gid, servers := range cfg.Groups {
		serversClone := make([]string, len(servers))
		copy(serversClone, servers)
		newConfig.Groups[gid] = serversClone
	}

	return newConfig
}

type Err string

const (
	OK             = "OK"
	ErrNotExecuted = "ErrNotExecuted"
)

type JoinArgs struct {
	GID     int64    // unique replica group ID
	Servers []string // group server ports
	ClerkId int64
	OpId    int
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GID     int64
	ClerkId int64
	OpId    int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard   int // shard id.
	GID     int64
	ClerkId int64
	OpId    int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num     int // config number.
	ClerkId int64
	OpId    int
}

type QueryReply struct {
	Config Config
	Err    Err
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
