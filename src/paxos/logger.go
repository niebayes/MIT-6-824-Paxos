package paxos

import (
	"fmt"
)

const DEBUG = true

func printf(format string, a ...interface{}) {
	// print iff debug is set.
	if DEBUG {
		format += "\n"
		fmt.Printf(format, a...)
	}
}
