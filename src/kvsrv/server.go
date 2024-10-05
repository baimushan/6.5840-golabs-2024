package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	kvmap map[string]string

	clerkIdxMap    map[int64]int64
	clerkIdxPreMap map[int64]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, exists := kv.kvmap[args.Key]; exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	//log.Printf("Get %v %v", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvmap[args.Key] = args.Value
	//log.Printf("Put %v %v", args.Key, args.Value)
	//kv.clerkIdxMap[args.ClerkIdx] = args.Idx
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	idx, exists := kv.clerkIdxMap[args.ClerkIdx]
	if exists && idx == args.Idx {
		//2024/10/05 20:23:09 Clerk 3876708967920890663  Idx 6 dup 3 x 3 5 y
		reply.Value = kv.clerkIdxPreMap[args.ClerkIdx]
		//log.Printf("Clerk %d  Idx %d dup %v %v ", args.ClerkIdx, args.Idx, args.Key, args.Value)

		return
	}

	if value, exists := kv.kvmap[args.Key]; exists {
		reply.Value = value
		kv.kvmap[args.Key] = kv.kvmap[args.Key] + args.Value
	} else {
		reply.Value = ""
		kv.kvmap[args.Key] = args.Value
	}
	kv.clerkIdxPreMap[args.ClerkIdx] = reply.Value
	kv.clerkIdxMap[args.ClerkIdx] = args.Idx
	//log.Printf("Clerk %d Idx %d Append %v %v %v", args.ClerkIdx, args.Idx, args.Key, args.Value, kv.kvmap[args.Key])
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvmap = make(map[string]string)
	kv.clerkIdxMap = make(map[int64]int64)
	kv.clerkIdxPreMap = make(map[int64]string)
	// You may need initialization code here.

	return kv
}
