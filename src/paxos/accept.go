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
	args := &AcceptArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: ins.propNum, Value: ins.propValue}
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
				args := &AcceptArgs{Me: px.me, SeqNum: ins.seqNum, PropNum: ins.propNum, Value: ins.propValue}
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

	// update max seen sequence number.
	px.maybeUpdateMaxSeenSeqNum(args.SeqNum)
	// update max seen proposal number.
	px.maybeUpdateMaxSeenPropNum(args.PropNum)

	reply.Err = OK
	reply.Me = px.me

	ins := px.getInstance(args.SeqNum)

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

	printf("S%v accepts proposal (N=%v P=%v V=%v) from S%v", px.me, args.SeqNum, args.PropNum, args.Value, args.Me)

	return nil
}

func (px *Paxos) handleAcceptReply(args *AcceptArgs, reply *AcceptReply) {
	px.mu.Lock()
	defer px.mu.Unlock()

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
		ins.acceptOK[reply.Me] = true
	}
}
