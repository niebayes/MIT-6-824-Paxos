package paxos

func (px *Paxos) sendDecide(peer, seqNum, propNum, doneSeqNum int, value interface{}) {
	args := &DecideArgs{Me: px.me, SeqNum: seqNum, PropNum: propNum, Value: value, DoneSeqNum: doneSeqNum}
	reply := &DecideReply{}
	if call(px.peers[peer], "Paxos.Decide", args, reply) {
		px.handleDecideReply(reply)
	} else {
		printf("S%v failed to send Decide to S%v", px.me, peer)
	}
}

func (px *Paxos) broadcastDecides(ins *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	for i := range px.peers {
		if i != px.me {
			// due to forgetting, the paxos instance may be deleted and hence the ins pointer may point to a nil.
			// for the sake of safety, we need to wrap the needed args into closure.
			go px.sendDecide(i, ins.seqNum, ins.propNum, px.maxDoneSeqNum[px.me], ins.propValue)
		} else {
			// make a local call if the receiver is myself.
			go func(seqNum, propNum, doneSeqNum int, value interface{}) {
				args := &DecideArgs{Me: px.me, SeqNum: seqNum, PropNum: propNum, Value: value, DoneSeqNum: doneSeqNum}
				reply := &DecideReply{}
				px.Decide(args, reply)
				px.handleDecideReply(reply)
			}(ins.seqNum, ins.propNum, px.maxDoneSeqNum[px.me], ins.propValue)
		}
	}
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update max seen sequence number.
	px.maybeUpdateMaxSeenSeqNum(args.SeqNum)
	// update max seen proposal number.
	px.maybeUpdateMaxSeenPropNum(args.PropNum)
	// update max done sequence number for the sender.
	px.maybeUpdateMaxDoneSeqNum(args.Me, args.DoneSeqNum)

	reply.Me = px.me
	reply.DoneSeqNum = px.maxDoneSeqNum[px.me]

	ins := px.getInstance(args.SeqNum)

	if ins.decidedValue != nil {
		printf("S%v rejects Decide (N=%v P=%v V=%v) from S%v since the value was decided (DV=%v)", px.me, args.SeqNum, args.PropNum, args.Value, args.Me, ins.decidedValue)
		return nil
	}

	// proposals of older rounds of paxos are rejected.
	// if the proposal number equals the maxSeenPreparePropNum, this means
	// this accept corresponds to the latest accepted prepare, and hence cannot be rejected.
	if ins.maxSeenPreparePropNum > args.PropNum {
		printf("S%v rejects Decide (N=%v P=%v V=%v) from S%v to keep the promise (MAP=%v)", px.me, args.SeqNum, args.PropNum, args.Value, args.Me, ins.maxSeenPreparePropNum)
		return nil
	}

	// set decided.
	ins.decidedValue = args.Value
	// update the promise.
	ins.maxSeenPreparePropNum = args.PropNum
	// update the latest accepted proposal.
	ins.maxSeenAcceptPropNum = args.PropNum
	ins.accpetedValue = args.Value

	printf("S%v decides proposal (N=%v P=%v V=%v) from S%v", px.me, args.SeqNum, args.PropNum, args.Value, args.Me)

	return nil
}

func (px *Paxos) handleDecideReply(reply *DecideReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.maybeUpdateMaxDoneSeqNum(reply.Me, reply.DoneSeqNum)
}
