package shardmaster

import (
	"sort"
)

// note: everything is pass-by-value in Go. However, the passed-by slice is a header which points
// to the backing array and any modification on the copied slice header will be made on the backing array.
func rebalanceShards(config *Config, isJoin bool, movedGid int64) {
	println("####################\nBefore rebalaning:")
	PrintGidToShards(config, DEBUG)

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

	gidAndShardsArray := make([]GidAndShards, 0)
	for gid, shards := range gidToShards {
		gidAndShardsArray = append(gidAndShardsArray, GidAndShards{gid: gid, shards: shards})
	}

	// sort the array by the number of shards in descending order.
	// warning: a stable sort is necessary since shard rebalancing must be deterministic so that
	// the rebalancing results are consistent though out all shardmaster servers.
	sort.SliceStable(gidAndShardsArray, func(i, j int) bool { return len(gidAndShardsArray[i].shards) > len(gidAndShardsArray[j].shards) })

	// compute the expected number of shards of each group after the rebalancing.
	numGroups := len(config.Groups)   // the Groups must have been added or removed the group.
	base := NShards / numGroups       // any group could get at least base shards.
	totalBonus := NShards % numGroups // only some groups could get bonus shards.

	if isJoin {
		// append an element for the new group so that the new group could be iterated
		gidAndShardsArray = append(gidAndShardsArray, GidAndShards{gid: movedGid, shards: make([]int, 0)})
	}
	expectedNumShardsOfGroup := make(map[int64]int)
	for _, gidAndShards := range gidAndShardsArray {
		if !isJoin && gidAndShards.gid == movedGid {
			// the leaved group has no shards after the rebalancing.
			expectedNumShardsOfGroup[movedGid] = 0
			continue
		}

		expectedNumShards := base
		if totalBonus > 0 {
			expectedNumShards += 1
			totalBonus -= 1
		}
		expectedNumShardsOfGroup[gidAndShards.gid] = expectedNumShards
	}

	from := make([]GidAndShards, 0) // shards from which group need to be moved out.
	to := make([]GidAndShards, 0)   // to which group shards need to be moved in.
	for i, gidAndShards := range gidAndShardsArray {
		gid := gidAndShards.gid
		currShards := gidAndShards.shards
		expectedNumShards := expectedNumShardsOfGroup[gid]
		diff := len(currShards) - expectedNumShards
		if diff > 0 {
			// for a group, if its current number of shards is greater than the expected number of shards after rebalancing,
			// then it shall give out the overflowed shards.

			// select the last diff shards as the overflowed shards.
			// note: many info, for e.g. the time the group serves the shard, the shard size, etc., could
			// be utilized to devise a better algorithm to select the overflowed shards. For now, the selection
			// algorithm is trivial.
			remainingShards := currShards[:expectedNumShards]
			movedOutShards := currShards[expectedNumShards:]
			from = append(from, GidAndShards{gid: gid, shards: movedOutShards})
			gidAndShardsArray[i].shards = remainingShards
		} else if diff < 0 {
			// for a group, if its current number of shards is less than the expected number of shards,
			// then some other groups shall hand off their overflowed shards to the group.

			// only the number of shards matters.
			to = append(to, GidAndShards{gid: gid, shards: make([]int, -diff)})
		}
	}

	// hand off overflowed shards to groups who need shards.
	movements := make([]Movement, 0)
	for i, toGidAndShards := range to {
		totalNeededNumShards := len(toGidAndShards.shards)
		cursor := 0

		for j, fromGidAndShards := range from {
			// no need to check if neededNumShards > 0 since the number of moved-in shards
			// must be equal to the number of moved-out shards.
			neededNumShards := totalNeededNumShards - cursor
			numMovedOutShards := min(len(fromGidAndShards.shards), neededNumShards)
			if numMovedOutShards <= 0 {
				// all shards are moved out, skip this group.
				continue
			}
			movedOutShards := fromGidAndShards.shards[:numMovedOutShards]

			if numMovedOutShards >= len(fromGidAndShards.shards) {
				from[j].shards = make([]int, 0)
			} else {
				from[j].shards = fromGidAndShards.shards[numMovedOutShards:]
			}

			for k := 0; k < numMovedOutShards; k++ {
				toGidAndShards.shards[cursor+k] = movedOutShards[k]
			}
			cursor += numMovedOutShards

			// record the movement.
			movements = append(movements, Movement{from: fromGidAndShards.gid, to: toGidAndShards.gid, shards: movedOutShards})
		}

		to[i] = toGidAndShards
	}

	println("Movements:")
	for _, movement := range movements {
		for _, shard := range movement.shards {
			config.Shards[shard] = movement.to
		}
		println("G%v -> G%v: %v", movement.from, movement.to, movement.shards)
	}

	println("After rebalaning:")
	PrintGidToShards(config, DEBUG)
	println("####################")
}
