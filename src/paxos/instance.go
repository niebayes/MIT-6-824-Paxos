package paxos

type Instance struct {
	status Fate // agreement status.
	seqNum int  // sequence number.

	// proposer fields.
	propNum                  int           // current proposal number.
	value                    interface{}   // value to propose.
	peerMaxSeenAcceptPropNum []int         // peers received highest proposal numbers.
	peerAcceptedValue        []interface{} // peers accepted values in the latest round.
	prepareOK                []bool        // true if the i-th peer has responded with prepare OK.
	acceptOK                 []bool        // true if the i-th peer has responded with accept OK.

	// acceptor fields.
	maxSeenPreparePropNum int
	maxSeenAcceptPropNum  int
	accpetedValue         interface{}
}

func makeInstance(seqNum int, value interface{}, numPeers int) *Instance {
	ins := &Instance{
		status:                   Pending,
		seqNum:                   seqNum,
		propNum:                  -1,
		value:                    value,
		peerMaxSeenAcceptPropNum: make([]int, numPeers),
		peerAcceptedValue:        make([]interface{}, numPeers),
		prepareOK:                make([]bool, numPeers),
		acceptOK:                 make([]bool, numPeers),
		maxSeenPreparePropNum:    -1,
		maxSeenAcceptPropNum:     -1,
		accpetedValue:            nil,
	}
	return ins
}
