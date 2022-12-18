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
	// key: client id, value: the id of the last successfully executed operation from this client.
	// this is used to ensure the at-most-once semantics, i.e. one operation could only be
	// executed by the pb server cluster at most once.
	// the reason why only the last executed op id is cached is that:
	// on the one hand, this lab assumes that there's at most one outstanding RPC,
	// i.e. an RPC which is omitted but not received, and hence there's no RPC duplication and reordering.
	// on the other hand, the failure scenarios are limited to server crash, server discard a request,
	// server executed the request but discard the reply.
	// for the first two scenarios, the client will repeat calling the server with the same op id since
	// the server does not respond before timeout.
	// for the last scenario, the client will also repeat calling the server with the same op id since
	// the server has discarded the reply.
	// if and only if the client receives a response with status OK or ErrNoKey (for Get), the client would
	// proceeds to sending the next request with an op id just one higher.
	// therefore, as for the single outstanding RPC request, its op id is limited to:
	// (1) normal case && first two failure scenarios: one higher than the cached exec op id.
	// (2) the last failure scenario: equal to the cached exec op id.
	lastExecOpId map[int64]uint
	// cached view.
	view viewservice.View
	// the address of the backup waiting for a copy of the key-value database of this server (the primary).
	transferee string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// handle this request only if this server is the current primary,
	// or this request is forwarded by the current primary.
	if pb.view.Primary != pb.me && !(pb.view.Primary == args.Primary && pb.view.Backup == pb.me) {
		reply.Err = ErrWrongServer
		maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
		return nil
	}

	// reject the request if being transfering state to the backup.
	if pb.transferee != "" {
		reply.Err = ErrInternal
		return nil
	}

	// reject the request if violates the at-most-once semantics.
	opId, exist := pb.lastExecOpId[args.Me]
	if exist && args.OpId <= opId {
		if args.OpId < opId {
			log.Fatalf("S%v C%v Shall not happen: args op id = %v  cached op id = %v", pb.me, args.Me, args.OpId, opId)
		}
		reply.Err = OK
		reply.Value, exist = pb.db[args.Key]
		if !exist {
			reply.Err = ErrNoKey
		}
		return nil
	}

	// forward the request to the backup.
	if pb.view.Backup != "" && pb.view.Backup != pb.me {
		maybePrintf("S%v forwarding Get (%v, %v) to S%v", pb.me, args.Key, args.OpId, pb.view.Backup)

		// do not make a gorouine to asyncly wait the RPC call,
		// since async may break the linearizability.
		if !call(pb.view.Backup, "PBServer.Get", args, reply) {
			// failed to contact with the backup.
			reply.Err = ErrInternal
		}
		if reply.Err != OK {
			// failed to sync the request with the backup.
			maybePrintf("S%v failed to sync Get (%v, %v) with S%v", pb.me, args.Key, args.OpId, pb.view.Backup)
			maybePrintf("Err = %v", reply.Err)
			maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
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
		maybePrintf("inconsistency found: Primary (%v, %v) Backup(%v, %v)", primReply.Value, primReply.Err, reply.Value, reply.Err)
		reply.Err = ErrInternal
		// start state transfering to sync the primary and the backup.
		// pb.transferee = pb.view.Backup
		return nil
	}

	// everything's ok.
	reply.Err = OK
	reply.Value = val
	maybePrintf("S%v executed Get (%v, %v) from C%v", pb.me, args.Key, args.OpId, args.Me)

	// update the latest executed operation id for this client.
	pb.lastExecOpId[args.Me] = args.OpId
	maybePrintf("S%v update cached op id for C%v to %v", pb.me, args.Me, args.OpId)

	// no error.
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// handle this request only if this server is the current primary,
	// or this request is forwarded by the current primary.
	if pb.view.Primary != pb.me && !(pb.view.Primary == args.Primary && pb.view.Backup == pb.me) {
		reply.Err = ErrWrongServer
		maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
		return nil
	}

	// reject the request if being transfering state to the backup.
	if pb.transferee != "" {
		maybePrintf("S%v is transfering state")
		reply.Err = ErrInternal
		return nil
	}

	// reject the request if violates the at-most-once semantics.
	opId, exist := pb.lastExecOpId[args.Me]
	if exist && args.OpId <= opId {
		if args.OpId < opId {
			log.Fatalf("S%v C%v Shall not happen: args op id = %v  cached op id = %v", pb.me, args.Me, args.OpId, opId)
		}
		reply.Err = OK
		return nil
	}

	// forward the request to the backup.
	if pb.view.Backup != "" && pb.view.Backup != pb.me {
		maybePrintf("S%v forwarding PutAppend (%v, %v, %v) to S%v", pb.me, args.Key, args.Value, args.OpId, pb.view.Backup)

		// do not make a gorouine to asyncly wait the RPC call.
		// since async may break the linearizability.
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
	var primReply *PutAppendReply = &PutAppendReply{}
	if args.Op == "Put" {
		// put.
		pb.db[args.Key] = args.Value
		primReply.Value = pb.db[args.Key]
	} else {
		// append.
		val := pb.db[args.Key]
		pb.db[args.Key] = val + args.Value
		primReply.Value = pb.db[args.Key]
	}
	primReply.Err = OK

	if pb.view.Primary == pb.me && pb.view.Backup != "" && (primReply.Value != reply.Value || primReply.Err != reply.Err) {
		// the primary and the backup returns different result, discard this request.
		maybePrintf("primary backup disagreement: Primary (%v, %v) Backup(%v, %v)", primReply.Value, primReply.Err, reply.Value, reply.Err)
		reply.Err = ErrInternal
		// start state transfering to sync the primary and the backup.
		// pb.transferee = pb.view.Backup
		return nil
	}

	// everything's ok.
	reply.Err = OK
	reply.Value = primReply.Value
	maybePrintf("S%v executed PutAppend (%v, %v, %v) from C%v", pb.me, args.Key, args.Value, args.OpId, args.Me)

	// update the latest executed operation id for this client.
	pb.lastExecOpId[args.Me] = args.OpId
	maybePrintf("S%v update cached op id for C%v to %v", pb.me, args.Me, args.OpId)

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
			pb.transferee = view.Backup
		} else {
			pb.transferee = ""
		}
		// update cached view.
		maybePrintf("S%v update view: (V%v, P%v, B%v) -> (V%v, P%v, B%v)", pb.me, pb.view.Viewnum, pb.view.Primary, pb.view.Backup, view.Viewnum, view.Primary, view.Backup)
		pb.view = view
	}

	// the current primary needs to transfer db state to the backup (if there's one).
	if pb.transferee != "" && pb.view.Primary == pb.me && pb.view.Backup != "" {
		maybePrintf("S%v transfering state to S%v", pb.me, pb.view.Backup)
		args := &TransferArgs{Me: pb.me, Db: pb.db, LastExecOpId: pb.lastExecOpId}
		reply := &TransferReply{}
		if call(pb.view.Backup, "PBServer.Transfer", args, reply) && (reply.Err == OK || reply.Err == ErrStale) {
			pb.transferee = ""
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
	pb.transferee = ""

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
					maybePrintf("S%v discard a request", pb.me)
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					maybePrintf("S%v discard a reply", pb.me)
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
