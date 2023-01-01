package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// paxos instances.
	// key: sequence number, value: paxos instance.
	instances map[int]*Instance
	// the highest proposal number this peer ever seen.
	maxSeenPropNum int
	// the i-th peer gets proposal numbers i, i + N, i + 2N, ... where N is len(peers), and i is the index into peers.
	// roundNum is the factor of N.
	// this allocation scheme ensures the uniqueness of proposal numbers for different proposers.
	roundNum int

	// the following fields are required by lab, not by paxos.

	// the highest sequence number this peer ever seen.
	maxSeenSeqNum int
	// instances with seq num <= maxForgottenSeqNum are forgotten.
	maxForgottenSeqNum int
	// the highest sequence numbers ever passed to Done for each peer.
	maxDoneSeqNum []int
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.mu = sync.Mutex{}

	px.instances = make(map[int]*Instance)
	px.maxSeenPropNum = -1
	px.roundNum = 0

	px.maxSeenSeqNum = -1
	px.maxForgottenSeqNum = -1
	px.maxDoneSeqNum = make([]int, len(px.peers))
	for i := range px.peers {
		px.maxDoneSeqNum[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for !px.isdead() {
				conn, err := px.l.Accept()
				if err == nil && !px.isdead() {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && !px.isdead() {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

func (px *Paxos) maybeUpdateMaxSeenSeqNum(seqNum int) {
	if seqNum > px.maxSeenSeqNum {
		px.maxSeenSeqNum = seqNum
		printf("S%v updates max seen sequence number to %v", px.me, seqNum)
	}
}

func (px *Paxos) maybeUpdateMaxDoneSeqNum(peer, seqNum int) {
	if seqNum > px.maxDoneSeqNum[peer] {
		px.maxDoneSeqNum[peer] = seqNum
		printf("S%v updates max done sequence number to %v for S%v", px.me, seqNum, peer)
	}
}

func (px *Paxos) maybeForget(seqNum int) {
	// collect all sequence numbers that need to be forgotten.
	forgottenSeqNums := make([]int, 0)
	for seq := range px.instances {
		if seq <= seqNum {
			forgottenSeqNums = append(forgottenSeqNums, seq)
		}
	}

	// delete instance state.
	for _, seq := range forgottenSeqNums {
		delete(px.instances, seq)

		if seq > px.maxForgottenSeqNum {
			px.maxForgottenSeqNum = seqNum
			printf("S%v forgets instances with sequence numbers <= %v", px.me, seqNum)
		}
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.maybeUpdateMaxSeenSeqNum(seq)

	// ignore forgotten proposals.
	if seq <= px.maxForgottenSeqNum {
		return
	}

	// start proposing the value if this paxos instance is not decided
	// and there's no goroutine being proposing this paxos instance.
	ins := px.getInstance(seq)
	if ins.decidedValue == nil && !ins.proposing {
		ins.proposing = true
		go px.propose(seq, v)
	}
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq <= px.maxForgottenSeqNum {
		return Forgotten, nil
	}

	ins := px.getInstance(seq)
	if ins.decidedValue == nil {
		return Pending, nil
	}

	return Decided, ins.decidedValue
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.maybeUpdateMaxDoneSeqNum(px.me, seq)

	minDoneSeqNum := 1 << 31
	for i := range px.peers {
		if px.maxDoneSeqNum[i] < minDoneSeqNum {
			minDoneSeqNum = px.maxDoneSeqNum[i]
		}
	}

	px.maybeForget(minDoneSeqNum)
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.maxSeenSeqNum
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	minDoneSeqNum := 1 << 31
	for i := range px.peers {
		if px.maxDoneSeqNum[i] < minDoneSeqNum {
			minDoneSeqNum = px.maxDoneSeqNum[i]
		}
	}

	px.maybeForget(minDoneSeqNum)

	return minDoneSeqNum + 1
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}
