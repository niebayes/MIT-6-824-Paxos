package paxos

import (
	"time"
)

func (px *Paxos) allocatePropNum() uint64 {
	px.mu.Lock()
	defer px.mu.Unlock()

	newPropNum := px.propNum
	for newPropNum < px.propNum {
		newPropNum = uint64(px.me) + px.roundNum*uint64(len(px.peers))
		px.roundNum++
	}
	return newPropNum
}

// return true if a majority of acceptors has been prepared for this instance.
func (px *Paxos) majorityPrepared(ins *Instance) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	numPrepared := 0
	for i := range px.peers {
		if ins.prepareOK[i] {
			numPrepared++
		}
	}

	return numPrepared*2 > len(px.peers)
}

func (px *Paxos) sendPrepare(peer int, ins *Instance) {
	args := &PrepareArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: px.propNum}
	reply := &PrepareReply{}
	if call(px.peers[peer], "Paxos.Prepare", args, reply) {
		px.handlePrepareReply(args, reply)
	} else {
		printf("S%v failed to send Prepare to S%v", px.me, peer)
	}
}

func (px *Paxos) broadcastPrepares(ins *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	for i := range px.peers {
		if ins.prepareOK[i] {
			continue
		}

		if i != px.me {
			go px.sendPrepare(i, ins)
		} else {
			// make a local call if the receiver is myself.
			go func() {
				args := &PrepareArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: px.propNum}
				reply := &PrepareReply{}
				px.Prepare(args, reply)
				px.handlePrepareReply(args, reply)
			}()
		}
	}
}

func (px *Paxos) doPrepare(ins *Instance) {
	// reset.
	px.mu.Lock()
	ins.rejected = false
	for i := range px.peers {
		ins.peerAcceptedProps[i] = nil
		ins.prepareOK[i] = false
		ins.acceptOK[i] = false
	}
	px.mu.Unlock()

	// choose a proposal number higher than the current one.
	px.propNum = px.allocatePropNum()

	printf("S%v starts doing prepare on proposal (N=%v P=%v V=%v)", px.me, ins.seqNum, px.propNum, ins.value)

	for !px.isdead() && px.isLeader() {
		if px.rejected(ins) {
			// this proposal was rejected, start a new round of proposal with a higher proposal number.
			printf("S%v knows proposal (N=%v P=%v V=%v) was rejected", px.me, ins.seqNum, px.propNum, ins.value)
			go px.doPrepare(ins)
			break
		}

		if px.majorityPrepared(ins) {
			// start the accept phase if a majority of peers have reported prepared, either by replying prepare OK or no more accepted.
			printf("S%v knows proposal (N=%v P=%v V=%v) was prepared", px.me, ins.seqNum, px.propNum, ins.value)
			go px.doAccept(ins)
			break
		}

		// send Prepare to unprepared peers.
		px.broadcastPrepares(ins)

		// check their responses later.
		time.Sleep(checkInterval)
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Me = px.me

	// reject if the sender is not the current leader.
	if args.Me != px.leader {
		printf("S%v rejects Prepare (N=%v P=%v) coz the sender S%v is not leader", px.me, args.SeqNum, args.PropNum, args.Me)
		reply.Err = ErrWrongLeader
		return nil
	}

	// reject if violates the promise.
	if px.minAcceptPropNum >= args.PropNum {
		printf("S%v rejects Prepare (N=%v P=%v) from S%v to keep the promise (MAP=%v)", px.me, args.SeqNum, args.PropNum, args.Me, px.minAcceptPropNum)
		reply.Err = ErrRejected
		return nil
	}

	// promise not to accept proposals with proposal numbers less than args.PropNum.
	px.minAcceptPropNum = args.PropNum

	reply.Err = OK
	reply.SeqNum = args.SeqNum
	// tell the proposer the highest-number proposal this acceptor has ever accepted.
	if args.SeqNum < len(px.instances) && px.instances[args.SeqNum] != nil {
		ins := px.instances[args.SeqNum]
		if ins.acceptedProp != nil {
			reply.AcceptedProp = ins.acceptedProp
		}
	}
	// check if there're no more accepted proposals with sequence numbers greater than args.SeqNum.
	reply.NoMoreAccepted = true
	if len(px.instances) > args.SeqNum {
		for _, ins := range px.instances[args.SeqNum+1:] {
			if ins.acceptedProp != nil {
				reply.NoMoreAccepted = false
				break
			}
		}
	}

	printf("S%v prepares for proposal (N=%v P=%v) from S%v", px.me, args.SeqNum, args.PropNum, args.Me)

	return nil
}

func (px *Paxos) handlePrepareReply(args *PrepareArgs, reply *PrepareReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	// discard the reply if not the leader.
	if px.leader != px.me {
		return
	}

	// discard the reply if changed proposal number.
	if args.PropNum != px.propNum {
		return
	}

	if reply.Err == ErrRejected {
		px.instances[args.SeqNum].rejected = true
	} else if reply.Err == OK {
		px.instances[args.SeqNum].prepareOK[reply.Me] = true
		px.instances[args.SeqNum].peerAcceptedProps[reply.Me] = reply.AcceptedProp
		px.noMoreAccepted[reply.Me] = reply.NoMoreAccepted
	}
}
