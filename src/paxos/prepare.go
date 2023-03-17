package paxos

import (
	"sync"
)

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

func (px *Paxos) sendPrepare(peer, seqNum, propNum int, wg *sync.WaitGroup) {
	args := &PrepareArgs{Me: px.me, SeqNum: seqNum, PropNum: propNum}
	reply := &PrepareReply{}
	if call(px.peers[peer], "Paxos.Prepare", args, reply) {
		px.handlePrepareReply(args, reply)
	} else {
		printf("S%v failed to send Prepare to S%v", px.me, peer)
	}

	wg.Done()
}

func (px *Paxos) broadcastPrepares(ins *Instance, done chan bool) {
	// bookeeping to workaround races.
	px.mu.Lock()
	seqNum := ins.seqNum
	propNum := ins.propNum
	px.mu.Unlock()

	wg := &sync.WaitGroup{}
	wg.Add(len(px.peers))

	for i := range px.peers {
		if i != px.me {
			go px.sendPrepare(i, seqNum, propNum, wg)
		} else {
			// make a local call if the receiver is myself.
			go func() {
				args := &PrepareArgs{Me: px.me, SeqNum: seqNum, PropNum: propNum}
				reply := &PrepareReply{}
				px.Prepare(args, reply)
				px.handlePrepareReply(args, reply)
				wg.Done()
			}()
		}
	}

	// wait responses of all peers.
	wg.Wait()

	done <- px.majorityPrepared(ins)
	close(done)
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update max seen sequence number.
	px.maybeUpdateMaxSeenSeqNum(args.SeqNum)
	// update max seen proposal number.
	px.maybeUpdateMaxSeenPropNum(args.PropNum)

	reply.Err = OK
	reply.Me = px.me
	reply.MaxSeenAcceptPropNum = -1
	reply.AcceptedValue = nil

	ins := px.getInstance(args.SeqNum)

	// proposals of older rounds of paxos are rejected.
	// if allowed to accept proposal from older rounds, then the quorum intersection property
	// will be violated since there could exist at most one accepted value at a time, and this
	// accepted value must be the value proposed in the last round.
	// only such the chosen value (if any) could be preserved.
	if ins.maxSeenPreparePropNum >= args.PropNum {
		printf("S%v rejects Prepare (N=%v P=%v) from S%v to keep the promise (MAP=%v)", px.me, args.SeqNum, args.PropNum, args.Me, ins.maxSeenPreparePropNum)
		reply.Err = ErrRejected
		return nil
	}

	// update the promise.
	ins.maxSeenPreparePropNum = args.PropNum
	// tell the proposer the accepted value of the latest round of paxos this peer even known.
	reply.MaxSeenAcceptPropNum = ins.maxSeenAcceptPropNum
	reply.AcceptedValue = ins.accpetedValue

	printf("S%v prepares for proposal (N=%v P=%v) from S%v", px.me, args.SeqNum, args.PropNum, args.Me)

	return nil
}

func (px *Paxos) handlePrepareReply(args *PrepareArgs, reply *PrepareReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update max seen proposal number.
	px.maybeUpdateMaxSeenPropNum(reply.MaxSeenAcceptPropNum)

	// discard the reply if the instance is forgotten.
	if args.SeqNum <= px.maxForgottenSeqNum {
		return
	}

	ins := px.getInstance(args.SeqNum)
	// discard the reply if the instance is decided.
	if ins.decidedValue != nil {
		return
	}

	// discard the reply if we're in a different round of paxos.
	if args.PropNum != ins.propNum {
		return
	}

	if reply.Err == OK {
		ins.prepareOK[reply.Me] = true
		ins.peerMaxSeenAcceptPropNum[reply.Me] = reply.MaxSeenAcceptPropNum
		ins.peerAcceptedValue[reply.Me] = reply.AcceptedValue
	}
}
