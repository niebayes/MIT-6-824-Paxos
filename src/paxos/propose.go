package paxos

import "time"

const backoffFactor = 2
const maxSleepTime = 300 * time.Millisecond
const proposeTimeout = 1 * time.Second

func (px *Paxos) maybeUpdateMaxSeenPropNum(propNum int) {
	if propNum > px.maxSeenPropNum {
		px.maxSeenPropNum = propNum
	}
}

// the proposal number is used to distinguish paxos rounds initiated either by this peer or other peers.
// therefore, to start a new round of paxos, a proposal number needs to be different than every
// other proposal number allocated thus far, i.e. each proposal number has to be unique.
// on the other hand, to ensure a proposal will be accepted, the proposal number needs to be higher than
// all proposal numbers allocated thus far, or the proposal will be rejected by some acceptors.
// in summary, we need to allocate a proposal greater than the highest proposal number this peer
// ever seen whenever we want to initiate a new round of paxos.
func (px *Paxos) choosePropNum() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	propNum := px.maxSeenPropNum
	for propNum <= px.maxSeenPropNum {
		propNum = px.me + px.roundNum*len(px.peers)
		px.roundNum++
	}

	px.maxSeenPropNum = propNum

	return propNum
}

// the value to be proposed in this round of paxos has to be the value accepted in the last round (if any).
// it's guaranteed by the quorum intersection property that if there's any chosen value, the accepted value
// in the last round must be this chosen value.
// since quorum intersection property is only hold between adjacent rounds of paxos, the value
// to be proposed must the accepted in the last round, i.e. the round with the highest accepted proposal number.
func (px *Paxos) choosePropValue(ins *Instance) {
	maxPropNum := -1
	for i := range px.peers {
		// warning, must consider the accepted proposal of our own since this value comes from others and it might be decided.
		if ins.prepareOK[i] && ins.peerMaxSeenAcceptPropNum[i] > maxPropNum && ins.peerAcceptedValue[i] != nil {
			maxPropNum = ins.peerMaxSeenAcceptPropNum[i]
			ins.propValue = ins.peerAcceptedValue[i]
		}
	}
}

func (px *Paxos) pending(seqNum int) bool {
	status, _ := px.Status(seqNum)
	return status == Pending
}

func (px *Paxos) propose(seqNum int, value interface{}) {
	// true if this the first try on proposing this instance.
	first := true
	lastSleepTime := 25 * time.Millisecond

	// continue proposing the value if this paxos instance is pending, i.e. not decided or forgotten.
	for !px.isdead() && px.pending(seqNum) {
		// backoff to not start a new round of proposal immediately.
		// this may give another peer proposing the same instance more chance to decide the value.
		// and hence the total RPC count could be reduced.
		if !first {
			sleepTime := lastSleepTime * backoffFactor
			if sleepTime > maxSleepTime {
				sleepTime = maxSleepTime
			}
			time.Sleep(sleepTime)
		}
		first = false

		px.mu.Lock()
		ins := px.getInstance(seqNum)
		px.resetInstance(ins, value)
		px.mu.Unlock()

		// choose a proposal number greater than the highest proposal number this peer ever seen.
		ins.propNum = px.choosePropNum()

		printf("S%v starts proposing (N=%v P=%v V=%v)", px.me, ins.seqNum, ins.propNum, ins.propValue)

		// broadcast prepares to all peers until timeout or majority prepared.
		done := make(chan bool)
		go px.broadcastPrepares(ins, done)

		select {
		case majorityPrepared := <-done:
			if !majorityPrepared {
				printf("S%v retries proposal (N=%v P=%v V=%v)", px.me, ins.seqNum, ins.propNum, ins.propValue)
				continue
			}
		case <-time.After(proposeTimeout):
			printf("S%v knows proposal (N=%v P=%v V=%v) timeouts", px.me, ins.seqNum, ins.propNum, ins.propValue)
			continue
		}

		printf("S%v knows proposal (N=%v P=%v V=%v) was prepared by a majority", px.me, ins.seqNum, ins.propNum, ins.propValue)

		// choose the proposed value with the highest proposal number among all prepared peers.
		// if there're none, choose our own value otherwise.
		px.choosePropValue(ins)

		// broadcast accepts to all peers until timeout or majority accepted.
		done = make(chan bool)
		go px.broadcastAccepts(ins, done)

		select {
		case majorityAccepted := <-done:
			if !majorityAccepted {
				printf("S%v retries proposal (N=%v P=%v V=%v)", px.me, ins.seqNum, ins.propNum, ins.propValue)
				continue
			}
		case <-time.After(proposeTimeout):
			printf("S%v knows proposal (N=%v P=%v V=%v) timeouts", px.me, ins.seqNum, ins.propNum, ins.propValue)
			continue
		}

		printf("S%v knows proposal (N=%v P=%v V=%v) was accepted by a majority", px.me, ins.seqNum, ins.propNum, ins.propValue)

		// set decided.
		px.mu.Lock()
		ins.decidedValue = ins.propValue
		ins.proposing = false
		printf("S%v finishes proposal (N=%v P=%v V=%v)", px.me, ins.seqNum, ins.propNum, ins.decidedValue)
		px.mu.Unlock()

		// broadcast decides to all peers.
		// if some peers are deaf currently, we leave them as they are.
		// the next round of paxos on the same sequence number will notify these peers.
		go px.broadcastDecides(ins)
		break
	}
}
