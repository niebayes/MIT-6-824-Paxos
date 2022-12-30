package paxos

type Instance struct {
	seqNum int // sequence number.

	// proposer fields.
	propNum                  int         // current proposal number.
	propValue                interface{} // value to propose.
	peerMaxSeenAcceptPropNum []int
	peerAcceptedValue        []interface{}
	prepareOK                []bool
	acceptOK                 []bool

	// acceptor fields.
	maxSeenPreparePropNum int
	maxSeenAcceptPropNum  int
	accpetedValue         interface{}

	// learner fields.
	decidedValue interface{} // decided value (if any).
}

func makeInstance(seqNum int, value interface{}, numPeers int) *Instance {
	ins := &Instance{
		seqNum: seqNum,

		// proposer fields.
		propNum:                  -1,
		propValue:                value,
		peerMaxSeenAcceptPropNum: make([]int, numPeers),
		peerAcceptedValue:        make([]interface{}, numPeers),
		prepareOK:                make([]bool, numPeers),
		acceptOK:                 make([]bool, numPeers),

		// acceptor fields.
		maxSeenPreparePropNum: -1,
		maxSeenAcceptPropNum:  -1,
		accpetedValue:         nil,

		// learner fields.
		decidedValue: nil,
	}
	return ins
}

// get instance by sequence number.
func (px *Paxos) getInstance(seqNum int) *Instance {
	ins, exist := px.instances[seqNum]
	// create the instance if does not exist.
	if !exist {
		px.instances[seqNum] = makeInstance(seqNum, nil, len(px.peers))
		ins = px.instances[seqNum]
	}
	return ins
}

func (px *Paxos) resetInstance(ins *Instance, propValue interface{}) {
	ins.propValue = propValue
	for i := range px.peers {
		ins.prepareOK[i] = false
		ins.acceptOK[i] = false
		ins.peerMaxSeenAcceptPropNum[i] = -1
		ins.peerAcceptedValue[i] = nil
	}
}
