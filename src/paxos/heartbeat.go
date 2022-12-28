package paxos

import (
	"time"
)

// heartbeat ticker.
func (px *Paxos) tick() {
	for !px.isdead() {
		// the max id among peers known to be alive currently.
		maxAliveId := px.me
		px.mu.Lock()
		for i := range px.peers {
			if i != px.me {
				// a peer is said to be alive if this peer has received its heartbeat recently.
				if time.Since(px.lastHeartbeatTime[i]) <= 2*TickInterval && i > maxAliveId {
					maxAliveId = i
				}

				go px.sendHeartbeat(i)
			}
		}
		// the leader is the alive peer with the highest id.
		px.leader = maxAliveId
		px.mu.Unlock()

		time.Sleep(TickInterval)
	}
}

func (px *Paxos) sendHeartbeat(peer int) {
	args := &HeartbeatArgs{Me: px.me}
	reply := &HeartbeatReply{}
	call(px.peers[peer], "Paxos.Heartbeat", args, reply)
}

// Heartbeat RPC handler.
func (px *Paxos) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update last heartbeat time.
	px.lastHeartbeatTime[args.Me] = time.Now()

	// update max done sequence number.
	// TODO.

	return nil
}
