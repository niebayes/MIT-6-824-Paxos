package shardmaster

import (
	"fmt"
	"sort"
)

const DEBUG = false

func println(format string, a ...interface{}) {
	// print iff debug is set.
	if DEBUG {
		format += "\n"
		fmt.Printf(format, a...)
	}
}

func PrintGidToShards(config *Config, debug bool) {
	if !debug {
		return
	}

	gidToShards := make(map[int64][]int)
	for shard, gid := range config.Shards {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// add mappings for groups with empty shards.
	for gid := range config.Groups {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = make([]int, 0)
		}
	}

	// convert map to slice to ensure the printing is in order.
	gidAndShardsArray := make([]GidAndShards, 0)
	for gid, shards := range gidToShards {
		gidAndShardsArray = append(gidAndShardsArray, GidAndShards{gid: gid, shards: shards})
	}

	// sort the array by group id in ascending order.
	sort.Slice(gidAndShardsArray, func(i, j int) bool { return gidAndShardsArray[i].gid < gidAndShardsArray[j].gid })

	for _, gidAndShards := range gidAndShardsArray {
		fmt.Printf("G%v: %v\n", gidAndShards.gid, gidAndShards.shards)
	}
}
