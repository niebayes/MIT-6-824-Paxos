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

	// current view, the view the servers will see.
	currentView View
	// latest view, the view the view service has ever seen.
	latestView View
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

func (vs *ViewServer) maybeRemoveServer(server string) {
	if vs.latestView.Primary == server {
		vs.latestView.Primary = ""
	}
	if vs.latestView.Backup == server {
		vs.latestView.Backup = ""
	}
	if vs.idleServer == server {
		vs.idleServer = ""
	}
}

func (vs *ViewServer) maybePromote() {
	// try to promote the backup to the primary (if there's no primary),
	// and try to promote the idle server to the backup (if there's a primary).

	// an idle server can be promoted to the backup if the followings are true:
	// (1) there's no backup.
	// (2) there's a primary.
	// the second condition bounds that an uninitialized idle server cannot become
	// the primary directly. If it could, then the previous data might get lost since
	// there's no primary and no backup, and hence no one could transfer data to the idle server.
	if vs.idleServer != "" && vs.latestView.Backup == "" && vs.latestView.Primary != "" {
		vs.latestView.Backup = vs.idleServer
		vs.idleServer = ""
	}

	if vs.latestView.Backup != "" && vs.latestView.Primary == "" {
		vs.latestView.Primary = vs.latestView.Backup
		vs.latestView.Backup = ""

		// promote the idle server (if there's one) to the backup.
		if vs.idleServer != "" {
			vs.latestView.Backup = vs.idleServer
			vs.idleServer = ""
		}
	}
}

func (vs *ViewServer) maybeSwitchView() {
	// switch to the latest view if the current view differs with the latest view
	// and if the primary of the current view has acked.
	if vs.currentView.Primary != vs.latestView.Primary || vs.currentView.Backup != vs.latestView.Backup {
		vs.latestView.Viewnum = vs.currentView.Viewnum + 1
		vs.currentView = vs.latestView
		// reset.
		vs.primaryAcked = false
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
	if vs.currentView.Viewnum == 0 {
		view := View{
			Viewnum: 1,
			Primary: server,
			Backup:  "",
		}
		vs.currentView = view
		vs.latestView = view
		reply.View = view
		return nil
	}

	// set primaryAcked to true if the pinging server is the primary of the current view and the ping
	// view number matches against the current view number.
	// it's possible that the primary of the current view dies but restarts in a short. The second
	// condition could rule out this possibility.
	if vs.currentView.Primary == server && vs.currentView.Viewnum == serverViewnum {
		vs.primaryAcked = true
	}

	if serverViewnum == 0 {
		// view number 0 indicates the server has just restarted or it's an idle server.
		vs.maybeRemoveServer(server)
	}

	if vs.primaryAcked {
		vs.maybePromote()
		vs.maybeSwitchView()
	}

	if serverViewnum == 0 {
		// the restarted server becomes the idle server.
		// we leave the idle server not involved in this round of promotion and view switching.
		// it could take part in the next round if it keeps pinging.
		// this makes the code simpler.
		vs.idleServer = server
	}

	// reply the server with the current view.
	reply.View = vs.currentView

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
	reply.View = vs.currentView

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
	for _, server := range deadServers {
		delete(vs.lastPingTime, server)
		vs.maybeRemoveServer(server)
	}

	if vs.primaryAcked {
		vs.maybePromote()
		vs.maybeSwitchView()
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
	vs.currentView = View{Viewnum: 0}
	vs.latestView = View{Viewnum: 0}
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
