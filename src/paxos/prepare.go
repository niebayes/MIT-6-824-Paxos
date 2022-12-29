package paxos

import (
	"sync"
)

// return true if a majority of acceptors has been prepared for this instance.
func (px *Paxos) majorityPrepared(ins *Instance) bool {
	numPrepared := 0
	for i := range px.peers {
		if ins.prepareOK[i] {
			numPrepared++
		}
	}

	return numPrepared*2 > len(px.peers)
}

func (px *Paxos) sendPrepare(peer int, ins *Instance, wg *sync.WaitGroup) {
	args := &PrepareArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: ins.propNum}
	reply := &PrepareReply{}
	if call(px.peers[peer], "Paxos.Prepare", args, reply) {
		px.handlePrepareReply(args, reply)
	} else {
		printf("S%v failed to send Prepare to S%v", px.me, peer)
	}

	wg.Done()
}

func (px *Paxos) broadcastPrepares(ins *Instance, done chan bool) {
	wg := &sync.WaitGroup{}
	wg.Add(len(px.peers))

	for i := range px.peers {
		if i != px.me {
			go px.sendPrepare(i, ins, wg)
		} else {
			// make a local call if the receiver is myself.
			go func() {
				args := &PrepareArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: ins.propNum}
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

	reply.Err = OK
	reply.Me = px.me
	reply.MaxSeenAcceptPropNum = -1
	reply.AcceptedValue = nil

	ins, exist := px.instances[args.SeqNum]
	if !exist {
		px.instances[args.SeqNum] = makeInstance(args.SeqNum, nil, len(px.peers))
		ins = px.instances[args.SeqNum]
	}

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
	// update max seen sequence number.
	px.maybeUpdateMaxSeenSeqNum(args.SeqNum)
	// update max seen proposal number.
	px.maybeUpdateMaxSeenPropNum(args.PropNum)

	printf("S%v prepares for proposal (N=%v P=%v) from S%v", px.me, args.SeqNum, args.PropNum, args.Me)

	return nil
}

func (px *Paxos) handlePrepareReply(args *PrepareArgs, reply *PrepareReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

	ins, exist := px.instances[args.SeqNum]
	// this paxos instance may be forgoteen, discard the reply.
	if !exist || args.SeqNum <= px.maxForgottenSeqNum {
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
