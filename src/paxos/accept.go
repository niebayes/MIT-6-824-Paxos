package paxos

import (
	"time"
)

// return true if a majority of acceptors has accepted this proposal.
func (px *Paxos) majorityAccepted(ins *Instance) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	numAccepted := 0
	for i := range px.peers {
		if ins.acceptOK[i] {
			numAccepted++
		}
	}

	return numAccepted*2 > len(px.peers)
}

func (px *Paxos) sendAccept(peer int, ins *Instance) {
	args := &AcceptArgs{Me: px.me, SeqNum: ins.seqNum, Prop: &Proposal{PropNum: px.propNum, Value: ins.value}}
	reply := &AcceptReply{}
	if call(px.peers[peer], "Paxos.Accept", args, reply) {
		px.handleAcceptReply(args, reply)
	} else {
		printf("S%v failed to send Accept to S%v", px.me, peer)
	}
}

func (px *Paxos) broadcastAccepts(ins *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	for i := range px.peers {
		if ins.acceptOK[i] {
			continue
		}

		if i != px.me {
			go px.sendAccept(i, ins)
		} else {
			// make a local call if the receiver is myself.
			go func() {
				args := &AcceptArgs{Me: px.me, SeqNum: ins.seqNum, Prop: &Proposal{PropNum: px.propNum, Value: ins.value}}
				reply := &AcceptReply{}
				px.Accept(args, reply)
				px.handleAcceptReply(args, reply)
			}()
		}
	}
}

func (px *Paxos) doAccept(ins *Instance) {
	// choose the proposed value with the highest proposal number.
	maxPropNum := uint64(0)
	for i := range px.peers {
		if ins.peerAcceptedProps[i] != nil && ins.peerAcceptedProps[i].PropNum > maxPropNum {
			ins.value = ins.peerAcceptedProps[i].Value
		}
	}

	printf("S%v starts doing accept on proposal (N=%v P=%v V=%v)", px.me, ins.seqNum, px.propNum, ins.value)

	// send Accept to peers that have not reported accept OK.
	for !px.isdead() && px.isLeader() {
		if px.rejected(ins) {
			// this proposal was rejected, start a new round of proposal with a higher proposal number.
			printf("S%v knows proposal (N=%v P=%v V=%v) was rejected", px.me, ins.seqNum, px.propNum, ins.value)
			go px.doPrepare(ins)
			break
		}

		if px.majorityAccepted(ins) {
			// start the decide phase if a majority of peers have reported accepted.
			printf("S%v knows proposal (N=%v P=%v V=%v) was decided", px.me, ins.seqNum, px.propNum, ins.value)
			ins.status = Decided
			go px.doDecide(ins)
			break
		}

		// send Accept to unaccepted peers.
		px.broadcastAccepts(ins)

		// check their responses later.
		time.Sleep(checkInterval)
	}
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Me = px.me

	// reject if the sender is not the current leader.
	if args.Me != px.leader {
		printf("S%v rejects Accept (N=%v P=%v V=%v) coz the sender S%v is not leader", px.me, args.SeqNum, args.Prop.PropNum, args.Prop.Value, args.Me)
		reply.Err = ErrWrongLeader
		return nil
	}

	// reject if violates the promise.
	if px.minAcceptPropNum > args.Prop.PropNum {
		printf("S%v rejects Prepare (N=%v P=%v V=%v) from S%v to keep the promise (MAP=%v)", px.me, args.SeqNum, args.Prop.PropNum, args.Prop.Value, args.Me, px.minAcceptPropNum)
		reply.Err = ErrRejected
		return nil
	}

	// promise not to accept proposals with proposal numbers less than args.PropNum.
	px.minAcceptPropNum = args.Prop.PropNum

	// extend the instances array if necessary.
	px.maybeExtendInstances(args.SeqNum)

	// create the instance if necessary.
	if px.instances[args.SeqNum] == nil {
		px.instances[args.SeqNum] = makeInstance(args.SeqNum, args.Prop.Value, len(px.peers))
	}

	// update the highest-number accepted proposal.
	// FIXME: Check the proposal number really goes higher.
	px.instances[args.SeqNum].acceptedProp = args.Prop

	reply.Err = OK
	reply.SeqNum = args.SeqNum

	printf("S%v accepts proposal (N=%v P=%v V=%v) from S%v", px.me, args.SeqNum, args.Prop.PropNum, args.Prop.Value, args.Me)

	return nil
}

func (px *Paxos) handleAcceptReply(args *AcceptArgs, reply *AcceptReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	// discard the reply if not the leader.
	if px.leader != px.me {
		return
	}

	// discard the reply if changed proposal number.
	if args.Prop.PropNum != px.propNum {
		return
	}

	if reply.Err == ErrRejected {
		px.instances[args.SeqNum].rejected = true
	} else if reply.Err == OK {
		px.instances[args.SeqNum].acceptOK[reply.Me] = true
	}
}
