package paxos

import (
	"time"
)

func (px *Paxos) sendRedirect(seqNum int, value interface{}) bool {
	args := &RedirectArgs{Me: px.me, SeqNum: seqNum, Value: value}
	reply := &RedirectReply{}
	if call(px.peers[px.leader], "Paxos.Redirect", args, reply) {
		return reply.Err == OK
	}

	printf("S%v failed to send Redirect to S%v", px.me, px.leader)
	return false
}

func (px *Paxos) redirectProposal(ins *Instance) {
	for !px.isdead() {
		// end redirection and start proposing if this server becomes the current leader.
		if px.isLeader() {
			go px.doPrepare(ins)
			break
		}

		// keep sending the proposal to the current leader until
		// it reports that it has received.
		if px.sendRedirect(ins.seqNum, ins.value) {
			break
		}

		time.Sleep(checkInterval)
	}
}

func (px *Paxos) Redirect(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Err = ErrRejected

	// reject the redirection if not the current leader.
	if px.leader != px.me {
		return nil
	}

	// extend the instances array if necessary.
	px.maybeExtendInstances(args.SeqNum)

	if px.instances[args.SeqNum] == nil {
		px.instances[args.SeqNum] = makeInstance(args.SeqNum, args.Prop.Value, len(px.peers))
		printf("S%v starts proposing (N=%v V=%v)", px.me, args.SeqNum, args.Prop.Value)
		go px.doPrepare(px.instances[args.SeqNum])
	}

	reply.Err = OK
	return nil
}
