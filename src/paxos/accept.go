package paxos

import (
	"sync"
)

// return true if a majority of acceptors has accepted this proposal.
func (px *Paxos) majorityAccepted(ins *Instance) bool {
	numAccepted := 0
	for i := range px.peers {
		if ins.acceptOK[i] {
			numAccepted++
		}
	}

	return numAccepted*2 > len(px.peers)
}

func (px *Paxos) sendAccept(peer int, ins *Instance, wg *sync.WaitGroup) {
	args := &AcceptArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: ins.propNum, Value: ins.value}
	reply := &AcceptReply{}
	if call(px.peers[peer], "Paxos.Accept", args, reply) {
		px.handleAcceptReply(args, reply)
	} else {
		printf("S%v failed to send Accept to S%v", px.me, peer)
	}

	wg.Done()
}

func (px *Paxos) broadcastAccepts(ins *Instance, done chan bool) {
	wg := &sync.WaitGroup{}
	wg.Add(len(px.peers))

	for i := range px.peers {
		if i != px.me {
			go px.sendAccept(i, ins, wg)
		} else {
			// make a local call if the receiver is myself.
			go func() {
				args := &AcceptArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: ins.propNum, Value: ins.value}
				reply := &AcceptReply{}
				px.Accept(args, reply)
				px.handleAcceptReply(args, reply)
				wg.Done()
			}()
		}
	}

	// wait responses of all peers.
	wg.Wait()

	done <- px.majorityAccepted(ins)
	close(done)
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Err = OK
	reply.Me = px.me

	ins, exist := px.instances[args.SeqNum]
	if !exist {
		px.instances[args.SeqNum] = makeInstance(args.SeqNum, nil, len(px.peers))
		ins = px.instances[args.SeqNum]
	}

	// proposals of older rounds of paxos are rejected.
	// if the proposal number equals the maxSeenPreparePropNum, this means
	// this accept corresponds to the latest accepted prepare, and hence cannot be rejected.
	if ins.maxSeenPreparePropNum > args.PropNum {
		printf("S%v rejects Accept (N=%v P=%v V=%v) from S%v to keep the promise (MAP=%v)", px.me, args.SeqNum, args.PropNum, args.Value, args.Me, ins.maxSeenPreparePropNum)
		reply.Err = ErrRejected
		return nil
	}

	// update the promise.
	ins.maxSeenPreparePropNum = args.PropNum
	// update the latest accepted proposal.
	ins.maxSeenAcceptPropNum = args.PropNum
	ins.accpetedValue = args.Value
	ins.value = args.Value
	// update max seen sequence number.
	px.maybeUpdateMaxSeenSeqNum(args.SeqNum)
	// update max seen proposal number.
	px.maybeUpdateMaxSeenPropNum(args.PropNum)

	printf("S%v accepts proposal (N=%v P=%v V=%v) from S%v", px.me, args.SeqNum, args.PropNum, args.Value, args.Me)

	return nil
}

func (px *Paxos) handleAcceptReply(args *AcceptArgs, reply *AcceptReply) {
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
		ins.acceptOK[reply.Me] = true
	}
}
