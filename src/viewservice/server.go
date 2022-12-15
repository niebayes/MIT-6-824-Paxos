package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// there're three cases the view service could generate a new view:
	// (1) the view service has not received a ping from a server (primary or backup) for DeadPings * PingInterval.
	// (2) the view service receives a ping with view number 0 which indicates the server crashed and restarted.
	// (3) there's no backup in the current view and the view service has received a ping from an idle server, i.e.
	//     neither the primary nor the backup.
	// In summary, any event that could lead to the change of the current view will drive the view service proceed to the
	// next view.
	// However, there's restriction that the view service could only change its view until it has received a ping from
	// the primary of the current view.

	// view number of the current view, the view the servers will see from the ping reply.
	currViewnum uint
	// view number of the latest view (not necessarily the next view), the view the view service has ever seen.
	latestViewnum uint
	// key: view number, value: the corresponding view.
	views map[uint]View
	// true if the primary of the current view has acked, i.e. the view service has received a ping
	// from the primary and the ping view number equals to the current view number.
	primaryAcked bool
	// key: server address, value: the last time receives a ping from the server.
	lastPingTime map[string]time.Time
	// a restarted server initially becomes the idle server.
	// the idle server is not included in a view and will be deleted if the view service
	// has not received a ping from it recently.
	// there may be more than one idle servers, but we only keep the one who pings
	// the view service most recently.
	idleServer string
}

func (vs *ViewServer) promote(latestView *View) bool {
	anyChange := false
	// promote the idle server (if there's one) to the backup (if there's no one).
	if vs.idleServer != "" && latestView.Backup == "" {
		maybePrintf("Promote %v Idle -> Back", vs.idleServer)
		latestView.Backup = vs.idleServer
		vs.idleServer = ""
		anyChange = true
	}

	// promote the backup (if there's one) to the primary (if there's no one).
	if latestView.Backup != "" && latestView.Primary == "" {
		maybePrintf("Promote %v Back -> Prim", latestView.Backup)
		latestView.Primary = latestView.Backup
		latestView.Backup = ""
		anyChange = true
	}
	return anyChange
}

func (vs *ViewServer) updateLatestView(latestView *View) {
	vs.latestViewnum += 1
	latestView.Viewnum = vs.latestViewnum
	prev := vs.views[vs.latestViewnum]
	vs.views[vs.latestViewnum] = *latestView
	maybePrintf("Update latest view (%v, %v, %v) -> (%v, %v, %v)", prev.Viewnum, prev.Primary, prev.Backup,
		latestView.Viewnum, latestView.Primary, latestView.Backup)
}

func (vs *ViewServer) maybeSwitchView(latestView *View) {
	// switch to the next view if there's one and if the primary of the current view has acked.
	if _, exist := vs.views[vs.currViewnum+1]; exist && vs.primaryAcked {
		vs.currViewnum += 1
		// reset.
		vs.primaryAcked = false
		curr := vs.views[vs.currViewnum]
		next := vs.views[vs.currViewnum+1]
		maybePrintf("Update current view (%v, %v, %v) -> (%v, %v, %v)", curr.Viewnum, curr.Primary, curr.Backup,
			next.Viewnum, next.Primary, next.Backup)
	}
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server := args.Me
	serverViewnum := args.Viewnum

	// the first ping the view service ever received incurs a view change from view 0 to view 1,
	// and the pinging server becomes the primary of view 1.
	if vs.currViewnum == 0 {
		vs.currViewnum = 1
		vs.latestViewnum = 1
		view := View{
			Viewnum: vs.currViewnum,
			Primary: server,
			Backup:  "",
		}
		vs.views[vs.currViewnum] = view
		reply.View = view
		return nil
	}

	// the latest view, i.e. the most recent roles the view service has assigned to servers.
	latestView := vs.views[vs.latestViewnum]
	anyChange := false

	// set primaryAcked to true if the pinging server is the primary of the current view and the ping
	// view number matches against the current view number.
	// it's possible that the primary of the current view dies but restarts in a short. The second
	// condition could rule out this possibility.
	if vs.views[vs.currViewnum].Primary == server && vs.currViewnum == serverViewnum {
		vs.primaryAcked = true
		maybePrintf("Primary S%v acked in view %v", server, serverViewnum)
	}

	if serverViewnum == 0 {
		// view number 0 indicates a restart of the server.
		// a restarted server becomes the idle server initially.
		vs.idleServer = server
		// remove the restarted server from the latest view (if it's in).
		if latestView.Primary == server {
			latestView.Primary = ""
			anyChange = true
		}
		if latestView.Backup == server {
			latestView.Backup = ""
			anyChange = true
		}

		maybePrintf("S%v becomes the idle server", server)
	}

	// if the latest view has any change, generate a new view.
	if vs.promote(&latestView) || anyChange {
		vs.updateLatestView(&latestView)
		vs.maybeSwitchView(&latestView)
	}

	// reply the server with the current view.
	reply.View = vs.views[vs.currViewnum]

	// update last ping time for this server.
	vs.lastPingTime[server] = time.Now()

	// no error.
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// reply the server with the current view.
	reply.View = vs.views[vs.currViewnum]

	// no error.
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// container to temporarily store the host addresses of dead servers.
	// a server is said to be dead if the view service has not received a ping from the server
	// for DeadPings * PingInterval time.
	deadServers := make([]string, 0)
	for server, last := range vs.lastPingTime {
		if time.Since(last) > DeadPings*PingInterval {
			deadServers = append(deadServers, server)
		}
	}

	// remove all dead servers.
	latestView := vs.views[vs.latestViewnum]
	anyChange := false
	for _, server := range deadServers {
		delete(vs.lastPingTime, server)

		if latestView.Primary == server {
			latestView.Primary = ""
			anyChange = true
		}
		if latestView.Backup == server {
			latestView.Backup = ""
			anyChange = true
		}
		if vs.idleServer == server {
			vs.idleServer = ""
		}

		maybePrintf("S%v is dead", server)
	}

	// if the latest view has any change, generate a new view.
	if vs.promote(&latestView) || anyChange {
		vs.updateLatestView(&latestView)
		vs.maybeSwitchView(&latestView)
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

// has this server been asked to shut down?
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.currViewnum = 0
	vs.latestViewnum = 0
	vs.views = make(map[uint]View)
	vs.views[vs.currViewnum] = View{}
	vs.primaryAcked = false
	vs.lastPingTime = make(map[string]time.Time)
	vs.idleServer = ""
	vs.mu = sync.Mutex{}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
