package shardmaster

import (
	"fmt"
)

const DEBUG = true

// FIXME: change printf to println since a new line is always appended.
func printf(format string, a ...interface{}) {
	// print iff debug is set.
	if DEBUG {
		format += "\n"
		fmt.Printf(format, a...)
	}
}
