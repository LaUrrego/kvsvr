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
	mu    sync.Mutex
	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.store[args.Key]; !ok {
		reply.Value = ""
	} else {
		reply.Value = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store[args.Key] = args.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	old := kv.store[args.Key]
	if _, ok := kv.store[args.Key]; !ok {
		kv.store[args.Key] = args.Value
	} else {
		kv.store[args.Key] = old + args.Value
	}
	reply.Value = old
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)

	return kv
}
