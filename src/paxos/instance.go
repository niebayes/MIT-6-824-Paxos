package paxos

type Proposal struct {
	PropNum uint64
	Value   interface{}
}

type Instance struct {
	status Fate // agreement status.
	seqNum int  // sequence number.
	// proposer fields.
	value             interface{} // value to propose by this peer.
	rejected          bool        // true if the previous round of proposing is rejected by some acceptors.
	peerAcceptedProps []*Proposal // peer proposals received from prepare replies.
	prepareOK         []bool      // true if the i-th peer has responded with prepare OK.
	acceptOK          []bool      // true if the i-th peer has responded with accept OK.
	decideOK          []bool      // true if the i-th peer has responded with decided OK.
	// acceptor fields.
	acceptedProp *Proposal // the latest accepted proposal.
}

func makeInstance(seqNum int, value interface{}, numPeers int) *Instance {
	ins := &Instance{
		status:            Pending,
		seqNum:            seqNum,
		value:             value,
		rejected:          false,
		peerAcceptedProps: make([]*Proposal, numPeers),
		prepareOK:         make([]bool, numPeers),
		acceptOK:          make([]bool, numPeers),
		decideOK:          make([]bool, numPeers),
		acceptedProp:      nil,
	}
	return ins
}

func (px *Paxos) maybeExtendInstances(seqNum int) {
	diff := len(px.instances) - seqNum - 1
	if diff < 0 {
		px.instances = append(px.instances, make([]*Instance, -diff)...)
	}
}
