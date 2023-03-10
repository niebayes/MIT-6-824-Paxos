package shardmaster

import "testing"

func TestRebalanceShards(t *testing.T) {
	config := Config{
		Num: 0,
		Groups: map[int64][]string{
			1: make([]string, 0),
			2: make([]string, 0),
			3: make([]string, 0),
		},
		Shards: [NShards]int64{
			1, 1, 1, 1, 2, 2, 2, 3, 3, 3,
		},
	}

	newGid := 4

	rebalanceShards(&config, int64(newGid))
}
