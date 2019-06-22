package raftkv

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	currentleader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.currentleader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//DPrintf("client get ")
	args := GetArgs{key, nrand()}
	ld := ck.currentleader
	for {
		reply := GetReply{}
		ok := ck.servers[ld].Call("KVServer.Get", &args, &reply)
		//DPrintf("rpc call returned")
		if ok {
			if reply.WrongLeader {
				ld = (ld + 1) % len(ck.servers)
			} else {
				//DPrintf("client get success")
				if reply.Err != "" {
					continue
				}
				ck.currentleader = ld
				return reply.Value
			}
		} else {
			ld = (ld + 1) % len(ck.servers)
		}
	}
	// You will have to modify this function.
	//return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//DPrintf("client put append")
	args := PutAppendArgs{key, value, op, nrand()}
	//DPrintf("%v", args)
	//DPrintf("key:%v, value:%v, op:%v", key, value, op)
	ld := ck.currentleader
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ld].Call("KVServer.PutAppend", &args, &reply)
		//DPrintf("rpc call %v returned : %v %v", args, ok, reply)
		if ok {
			if reply.WrongLeader {
				ld = (ld + 1) % len(ck.servers)
			} else {
				//DPrintf("Client Put Append Success")
				if reply.Err != "" {
					continue
				}
				ck.currentleader = ld
				return
			}
		} else {
			ld = (ld + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
