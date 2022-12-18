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
	// key: client id, value: the id of the next to-be-executed operation from this client.
	// this is used to ensure the at-most-once semantics, i.e. one operation could only be
	// executed by the pb server cluster at most once.
	// if and only if the args' OpId matches with the nextOpId, shall a request be accepted.
	nextOpId map[int64]uint
	// cached view.
	view viewservice.View
	// the address of the backup waiting for a copy of the key-value database of this server (the primary).
	transferee string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	maybePrintf("S%v receives Get (%v, %v) from C%v", pb.me, args.Key, args.OpId, args.Me)

	// handle this request only if this server is the current primary,
	// or this request is forwarded by the current primary and this server is the current backup.
	if pb.view.Primary != pb.me && !(pb.view.Primary == args.Primary && pb.view.Backup == pb.me) {
		maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
		reply.Err = ErrWrongServer
		return nil
	}

	// reject the request if being transfering state to the backup.
	if pb.transferee != "" {
		maybePrintf("S%v is transfering to S%v", pb.me, pb.transferee)
		reply.Err = ErrInternal
		return nil
	}

	// reject the request if violates the at-most-once semantics.
	opId := pb.nextOpId[args.Me]
	if args.OpId != opId {
		if args.OpId < opId {
			// this operation was executed, try to fetch the value from the database.
			reply.Err = OK
			val, exist := pb.db[args.Key]
			if exist {
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
			}
			maybePrintf("S%v with next op id %v detects Get (%v, %v) as duplicate", pb.me, opId, args.Key, args.OpId)
			return nil
		} else {
			// this server lags behind.
			// it might because this server is an uninitialized backup and is waiting for a copy
			// of the database from the primary.
			maybePrintf("S%v with next op id %v detects Get (%v, %v) as forward", pb.me, opId, args.Key, args.OpId)
			reply.Err = ErrInternal
			return nil
		}
	}

	// forward the request to the backup.
	if pb.view.Backup != "" && pb.view.Backup != pb.me {
		maybePrintf("S%v forwarding Get (%v, %v) to S%v", pb.me, args.Key, args.OpId, pb.view.Backup)

		// do not make a gorouine to asyncly wait the RPC call,
		// since async may break the linearizability.
		if !call(pb.view.Backup, "PBServer.Get", args, reply) {
			// failed to contact with the backup.
			maybePrintf("S%v failed to contact with the backup %v", pb.me, pb.view.Backup)
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
	// as the semantics of Get, empty string indicates a non-exist key.
	if exist && val != "" {
		primReply.Err = OK
		primReply.Value = val
	} else {
		primReply.Err = ErrNoKey
	}

	if pb.view.Primary == pb.me && pb.view.Backup != "" && (primReply.Value != reply.Value || primReply.Err != reply.Err) {
		// the primary and the backup returns different results, discard this request.
		maybePrintf("Get inconsistency: Primary (%v, %v) Backup(%v, %v)", primReply.Value, primReply.Err, reply.Value, reply.Err)
		reply.Err = ErrInternal
		// the next tick will start state transfering to sync the primary and the backup.
		pb.transferee = pb.view.Backup
		return nil
	}

	// everything's ok.
	reply.Err = OK
	reply.Value = val
	maybePrintf("S%v executed Get (%v, %v) from C%v", pb.me, args.Key, args.OpId, args.Me)

	// increment the next op id.
	pb.nextOpId[args.Me] = opId + 1
	maybePrintf("S%v update cached op id for C%v to %v", pb.me, args.Me, args.OpId)

	// no error.
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	maybePrintf("S%v receives PutAppend (%v, %v, %v) from C%v", pb.me, args.Key, args.Value, args.OpId, args.Me)

	// handle this request only if this server is the current primary,
	// or this request is forwarded by the current primary and this server is the current backup.
	if pb.view.Primary != pb.me && !(pb.view.Primary == args.Primary && pb.view.Backup == pb.me) {
		maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
		reply.Err = ErrWrongServer
		return nil
	}

	// reject the request if being transfering state to the backup.
	if pb.transferee != "" {
		maybePrintf("S%v is transfering to S%v", pb.me, pb.transferee)
		reply.Err = ErrInternal
		return nil
	}

	// reject the request if violates the at-most-once semantics.
	opId := pb.nextOpId[args.Me]
	if args.OpId != opId {
		if args.OpId < opId {
			// this operation was executed, no need to fetch the value.
			maybePrintf("S%v with next op id %v detects PutAppend (%v, %v, %v) as duplicate", pb.me, opId, args.Key, args.Value, args.OpId)
			reply.Err = OK
			return nil
		} else {
			// this server lags behind.
			// it might because this server is an uninitialized backup and is waiting for a copy
			// of the database from the primary.
			maybePrintf("S%v with next op id %v detects PutAppend (%v, %v, %v) as forward", pb.me, opId, args.Key, args.Value, args.OpId)
			reply.Err = ErrInternal
			return nil
		}
	}

	// forward the request to the backup.
	if pb.view.Backup != "" && pb.view.Backup != pb.me {
		maybePrintf("S%v forwarding PutAppend (%v, %v, %v) to S%v", pb.me, args.Key, args.Value, args.OpId, pb.view.Backup)

		// do not make a gorouine to asyncly wait the RPC call.
		// since async may break the linearizability.
		if !call(pb.view.Backup, "PBServer.PutAppend", args, reply) {
			// failed to contact with the backup.
			maybePrintf("S%v failed to contact with the backup %v", pb.me, pb.view.Backup)
			reply.Err = ErrInternal
		}
		if reply.Err != OK {
			// failed to sync the request with the backup.
			maybePrintf("S%v failed to sync PutAppend (%v, %v, %v) with S%v", pb.me, args.Key, args.Value, args.OpId, pb.view.Backup)
			maybePrintf("Err = %v", reply.Err)
			maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
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
	} else {
		// append.
		// warning: it safe to simply use the val without checking its existence.
		// golang will return the default value for a non-exist key according to the value type.
		val := pb.db[args.Key]
		pb.db[args.Key] = val + args.Value
	}
	primReply.Value = pb.db[args.Key]
	primReply.Err = OK

	if pb.view.Primary == pb.me && pb.view.Backup != "" && primReply.Err != reply.Err {
		// the primary and the backup returns different results, discard this request.
		maybePrintf("PutAppend inconsistency: Primary (%v, %v) Backup(%v, %v)", primReply.Value, primReply.Err, reply.Value, reply.Err)
		reply.Err = ErrInternal
		// the next tick will start state transfering to sync the primary and the backup.
		pb.transferee = pb.view.Backup
		return nil
	}

	// everything's ok.
	reply.Err = OK
	// recording the PutAppend value is required to check primary-backup consistency.
	reply.Value = primReply.Value
	maybePrintf("S%v executed PutAppend (%v, %v, %v) from C%v", pb.me, args.Key, args.Value, args.OpId, args.Me)

	// increment the next op id.
	pb.nextOpId[args.Me] = opId + 1
	maybePrintf("S%v update cached op id for C%v to %v", pb.me, args.Me, args.OpId)

	// no error.
	return nil
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// reject this RPC if the from server is not the current primary or if this server is not
	// the current backup.
	if pb.view.Primary != args.Me || pb.view.Backup != pb.me {
		maybePrintf("S%v rejects state transfered from S%v", pb.me, args.Me)
		maybePrintf("S%v's view: (P:%v, B:%v)", pb.me, pb.view.Primary, pb.view.Backup)
		reply.Err = ErrWrongServer
		return nil
	}

	// install the transfered state.
	pb.db = args.Db
	pb.nextOpId = args.NextOpId
	reply.Err = OK

	maybePrintf("S%v installed state transfered from S%v", pb.me, args.Me)

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

	// if detects view change, update the cached view.
	if err == nil && (view.Viewnum != pb.view.Viewnum || view.Primary != pb.view.Primary || view.Backup != pb.view.Backup) {
		if view.Primary == pb.me && view.Backup != "" {
			// notify the primary to transfer state to the backup.
			pb.transferee = view.Backup
		} else {
			pb.transferee = ""
		}
		maybePrintf("S%v update view: (V%v, P%v, B%v) -> (V%v, P%v, B%v)", pb.me, pb.view.Viewnum, pb.view.Primary, pb.view.Backup, view.Viewnum, view.Primary, view.Backup)
		pb.view = view
	}

	// the current primary needs to transfer database state to the backup (if there's one).
	if pb.transferee != "" && pb.view.Primary == pb.me && pb.view.Backup != "" {
		maybePrintf("S%v transfering state to S%v", pb.me, pb.view.Backup)

		// it's required to transfer the next op id by the way
		// since it's necessary to ensure the at-most-once semantics.
		args := &TransferArgs{Me: pb.me, Db: pb.db, NextOpId: pb.nextOpId}
		reply := &TransferReply{}

		if call(pb.view.Backup, "PBServer.Transfer", args, reply) && reply.Err == OK {
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
	pb.nextOpId = make(map[int64]uint)
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
		for !pb.isdead() {
			conn, err := pb.l.Accept()
			if err == nil && !pb.isdead() {
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
			if err != nil && !pb.isdead() {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for !pb.isdead() {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
