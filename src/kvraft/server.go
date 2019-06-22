package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operate string
	Key     string
	Value   string
	OpNum   int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data    map[string]string
	applyed map[int64]string

	cancelSig chan int
	applySig  chan int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server get: ")
	_, _, isleader := kv.rf.Start(Op{Operate: "Get", Key: args.Key, Value: "", OpNum: args.OpNum})
	if isleader {
		time.Sleep(2e8)
		if v, ok := kv.applyed[args.OpNum]; ok {
			reply.WrongLeader = false
			reply.Err = ""
			reply.Value = v
		} else {
			_, isl := kv.rf.GetState()
			reply.WrongLeader = !isl
			reply.Err = Err("time out")
			reply.Value = ""
		}
	} else {
		reply.WrongLeader = true
		reply.Err = Err("Wrong Leader")
		reply.Value = ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server %v", args)
	_, _, isleader := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.OpNum})
	if isleader {
		time.Sleep(2e8)
		if _, ok := kv.applyed[args.OpNum]; ok {
			reply.WrongLeader = false
			reply.Err = ""
		} else {
			_, isl := kv.rf.GetState()
			reply.WrongLeader = !isl
			reply.Err = "time out"
		}
	} else {
		reply.WrongLeader = true
		reply.Err = Err("Wrong Leader")
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.cancelSig <- 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.applyed = make(map[int64]string)
	kv.cancelSig = make(chan int)
	kv.applySig = make(chan int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go func() {
		for {
			select {
			case <-kv.cancelSig:
				return
			case msg := <-kv.applyCh:
				DPrintf("get commit msg")
				ops, ok := msg.Command.(Op)
				DPrintf("%v", ops)
				if ok {
					if _, ok := kv.applyed[ops.OpNum]; ok {
						break
					}
					if ops.Operate == "Get" {
						// do nothing
					} else if ops.Operate == "Put" {
						kv.data[ops.Key] = ops.Value
						DPrintf("kv data %v:%v", ops.Key, kv.data[ops.Key])
					} else if ops.Operate == "Append" {
						if _, e := kv.data[ops.Key]; e {
							kv.data[ops.Key] += ops.Value
						} else {
							kv.data[ops.Key] = ops.Value
						}
					}
					DPrintf("%v", kv.data)
					DPrintf("applyed : %v", kv.applyed)
					kv.mu.Lock()
					kv.applyed[ops.OpNum] = kv.data[ops.Key]
					kv.mu.Unlock()
					//_, isleader := kv.rf.GetState()
					//if isleader {
					//	kv.applySig <- 1
					//}
					DPrintf("out")
				}

				break
			}
		}
	}()

	return kv
}
