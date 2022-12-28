package paxos

import (
	"time"
)

// return true if all acceptors have been notified this instance was decided.
func (px *Paxos) allDecided(ins *Instance) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	numDecided := 0
	for i := range px.peers {
		if ins.decideOK[i] {
			numDecided++
		}
	}

	return numDecided == len(px.peers)
}

func (px *Paxos) sendDecide(peer int, ins *Instance) {
	args := &DecideArgs{Me: px.me, SeqNum: ins.seqNum, Prop: &Proposal{PropNum: px.propNum, Value: ins.value}}
	reply := &DecideReply{}
	if call(px.peers[peer], "Paxos.Decide", args, reply) {
		px.handleDecideReply(args, reply)
	} else {
		printf("S%v failed to send Decide to S%v", px.me, peer)
	}
}

func (px *Paxos) broadcastDecides(ins *Instance) {
	px.mu.Lock()
	defer px.mu.Unlock()

	for i := range px.peers {
		if ins.decideOK[i] {
			continue
		}

		if i != px.me {
			go px.sendDecide(i, ins)
		} else {
			// make a local call if the receiver is myself.
			go func() {
				args := &DecideArgs{Me: px.me, SeqNum: ins.seqNum, Prop: &Proposal{PropNum: px.propNum, Value: ins.value}}
				reply := &DecideReply{}
				px.Decide(args, reply)
				px.handleDecideReply(args, reply)
			}()
		}
	}
}

func (px *Paxos) doDecide(ins *Instance) {
	for !px.isdead() {
		if !px.isLeader() {
			// redirect the proposal to the current leader.
			printf("S%v starts redirecting (N=%v V=%v)", px.me, ins.seqNum, ins.value)
			go px.redirectProposal(ins)
			break
		}

		if px.rejected(ins) {
			// increase the proposal number to let the acceptor not reject the Decide msg.
			px.propNum = px.allocatePropNum()
		}

		if px.allDecided(ins) {
			printf("S%v knows proposal (N=%v P=%v V=%v) was all decided", px.me, ins.seqNum, px.propNum, ins.value)
			break
		}

		// send Decide to undecided peers.
		px.broadcastDecides(ins)

		// check their responses later.
		time.Sleep(checkInterval)
	}
}

// TODO: wrap Accept and Decide handlers to a common one.
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Me = px.me

	// reject if the sender is not the current leader.
	if args.Me != px.leader {
		printf("S%v rejects Decide (N=%v P=%v V=%v) coz the sender S%v is not leader", px.me, args.SeqNum, args.Prop.PropNum, args.Prop.Value, args.Me)
		reply.Err = ErrWrongLeader
		return nil
	}

	// reject if violates the promise.
	if px.minAcceptPropNum > args.Prop.PropNum {
		printf("S%v rejects Decide (N=%v P=%v V=%v) from S%v to keep the promise (MAP=%v)", px.me, args.SeqNum, args.Prop.PropNum, args.Prop.Value, args.Me, px.minAcceptPropNum)
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
	px.instances[args.SeqNum].status = Decided

	reply.Err = OK
	reply.SeqNum = args.SeqNum

	printf("S%v decides proposal (N=%v P=%v V=%v) from S%v", px.me, args.SeqNum, args.Prop.PropNum, args.Prop.Value, args.Me)

	return nil
}

func (px *Paxos) handleDecideReply(args *DecideArgs, reply *DecideReply) {
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
		px.instances[args.SeqNum].decideOK[reply.Me] = true
	}
}
