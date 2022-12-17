package pbservice

import (
	"6.824/src/viewservice"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const MAX_WAIT_TIME time.Duration = time.Second

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	// through which the pb server communicates with the view service.
	vs *viewservice.Clerk
	// key value database.
	db map[string]string
	// key: client address, value: the id of the last processed operation from this client.
	// this is used to ensure the at-most-once semantics, i.e. one operation could only be
	// executed by the pb server at most once.
	lastExecOpId map[int64]uint
	// cached view.
	view viewservice.View
	// true if a pending transfer needs to be sent from this server (the primary) to the backup.
	pendingTransfer bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// handle this request only if this server is the current primary,
	// or this request is forwarded by the current primary.
	if !(pb.view.Primary == pb.me || pb.view.Primary == args.Primary) {
		reply.Err = ErrWrongServer
		return nil
	}

	// reject the request if violates the at-most-once semantics.
	opId, exist := pb.lastExecOpId[args.Me]
	if exist && args.OpId <= opId {
		reply.Err = ErrDuplicate
		return nil
	}

	// reject the request if being transfering state to the backup.
	if pb.pendingTransfer {
		reply.Err = ErrInternal
		return nil
	}

	// forward the request to the backup.
	if pb.view.Backup != "" && pb.view.Backup != pb.me {
		maybePrintf("S%v forwarding Get (%v, %v) to S%v", pb.me, args.Key, args.OpId, pb.view.Backup)

		// do not make a gorouine to asyncly wait the RPC call,
		// since async may break the linearizability.
		args.Primary = pb.me
		if !call(pb.view.Backup, "PBServer.Get", args, reply) {
			// failed to contact with the backup.
			reply.Err = ErrInternal
		}
		if reply.Err != OK {
			// failed to sync the request with the backup.
			maybePrintf("Err = %v", reply.Err)
			maybePrintf("S%v failed to sync Get (%v, %v) with S%v", pb.me, args.Key, args.OpId, pb.view.Backup)
			return nil
		}
		maybePrintf("S%v successfully synced Get (%v, %v) with S%v", pb.me, args.Key, args.OpId, pb.view.Backup)
	}

	// the backup (if there's one) has successfully executed this request,
	// it's time for the primary to execute the operation.
	val, exist := pb.db[args.Key]
	var primReply *GetReply = &GetReply{}
	if exist {
		primReply.Err = OK
		primReply.Value = val
	} else {
		primReply.Err = ErrNoKey
	}
	if pb.view.Primary == pb.me && pb.view.Backup != "" && (primReply.Value != reply.Value || primReply.Err != reply.Err) {
		// the primary and the backup returns different result, discard this request.
		// FIXME: How to properly handle such inconsistency?
		maybePrintf("inconsistency found: Primary (%v, %v) Backup(%v, %v)", primReply.Value, primReply.Err, reply.Value, reply.Err)
		reply.Err = ErrInternal
		return nil
	}

	// everything's ok.
	reply.Err = OK
	reply.Value = val

	// update the latest executed operation id for this client.
	pb.lastExecOpId[args.Me] = args.OpId

	maybePrintf("S%v executed Get (%v, %v) from C%v", pb.me, args.Key, args.OpId, args.Me)

	// no error.
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// handle this request only if this server is the current primary,
	// or this request is forwarded by the current primary.
	if !(pb.view.Primary == pb.me || pb.view.Primary == args.Primary) {
		reply.Err = ErrWrongServer
		return nil
	}

	// reject the request if violates the at-most-once semantics.
	opId, exist := pb.lastExecOpId[args.Me]
	if exist && args.OpId <= opId {
		reply.Err = ErrDuplicate
		return nil
	}

	// reject the request if being transfering state to the backup.
	if pb.pendingTransfer {
		reply.Err = ErrInternal
		return nil
	}

	// forward the request to the backup.
	if pb.view.Backup != "" && pb.view.Backup != pb.me {
		maybePrintf("S%v forwarding PutAppend (%v, %v, %v) to S%v", pb.me, args.Key, args.Value, args.OpId, pb.view.Backup)

		// do not make a gorouine to asyncly wait the RPC call.
		// since async may break the linearizability.
		args.Primary = pb.me
		if !call(pb.view.Backup, "PBServer.PutAppend", args, reply) {
			// failed to contact with the backup.
			reply.Err = ErrInternal
		}
		if reply.Err != OK {
			// failed to sync the request with the backup.
			maybePrintf("Err = %v", reply.Err)
			maybePrintf("S%v failed to sync PutAppend (%v, %v, %v) with S%v", pb.me, args.Key, args.Value, args.OpId, pb.view.Backup)
			return nil
		}
		maybePrintf("S%v successfuly synced PutAppend (%v, %v, %v) with S%v", pb.me, args.Key, args.Value, args.OpId, pb.view.Backup)
	}

	// the backup (if there's one) has successfully executed this request,
	// it's time for the primary to execute the operation.
	if args.Op == "Put" {
		// put.
		pb.db[args.Key] = args.Value
	} else {
		// append.
		val := pb.db[args.Key]
		pb.db[args.Key] = val + args.Value
	}

	// everything's ok.
	reply.Err = OK

	// update the latest executed operation id for this client.
	pb.lastExecOpId[args.Me] = args.OpId

	maybePrintf("S%v executed PutAppend (%v, %v, %v) from C%v", pb.me, args.Key, args.Value, args.OpId, args.Me)

	// no error.
	return nil
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// reject this RPC if the from server is not the current primary or if this server not
	// the current backup.
	if pb.view.Primary != args.Me || pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	// install the transfered state.
	pb.db = args.Db
	pb.lastExecOpId = args.LastExecOpId
	reply.Err = OK

	maybePrintf("S%v installed state from S%v", pb.me, args.Me)

	// no error.
	return nil
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	maybePrintf("S%v pings with view number %v", pb.me, pb.view.Viewnum)
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil && (view.Viewnum != pb.view.Viewnum || view.Primary != pb.view.Primary || view.Backup != pb.view.Backup) {
		if view.Primary == pb.me && view.Backup != "" {
			// notify this server to send transfer to the backup periodically until the backup installed the state.
			pb.pendingTransfer = true
		} else {
			pb.pendingTransfer = false
		}
		// update cached view.
		pb.view = view
		maybePrintf("S%v update view to (V%v, P%v, B%v)", pb.me, pb.view.Viewnum, pb.view.Primary, pb.view.Backup)
	}

	// the current primary needs to transfer db state to the backup (if there's one).
	if pb.pendingTransfer && pb.view.Primary == pb.me && pb.view.Backup != "" {
		maybePrintf("S%v transfering state to S%v", pb.me, pb.view.Backup)
		args := &TransferArgs{Me: pb.me, Db: pb.db, LastExecOpId: pb.lastExecOpId}
		reply := &TransferReply{}
		if call(pb.view.Backup, "PBServer.Transfer", args, reply) && (reply.Err == OK || reply.Err == ErrStale) {
			pb.pendingTransfer = false
			maybePrintf("S%v successefully synced with S%v", pb.me, pb.view.Backup)
		} else {
			maybePrintf("S%v failed to transfer state to S%v", pb.me, pb.view.Backup)
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.db = make(map[string]string)
	pb.lastExecOpId = make(map[int64]uint)
	pb.view = viewservice.View{Viewnum: 0}
	pb.pendingTransfer = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
