package shardkv

import (
	"fmt"
)

const DEBUG = true

// FIXME: change println to println since a new line is always appended.
func println(format string, a ...interface{}) {
	// print iff debug is set.
	if DEBUG {
		format += "\n"
		fmt.Printf(format, a...)
	}
}
