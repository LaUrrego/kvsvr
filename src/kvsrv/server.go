package kvsrv

import (
	"log"
	"sync"
)

type Operation int

const (
	GET Operation = iota
	PUTAPPEND
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
	// keep track of clients and last reply sent
	clientMap     map[int64]int64
	appendRecords map[int64]string
}

func (kv *KVServer) isDuplicate(client int64, seq int64) bool {
	if lastSeq, ok := kv.clientMap[client]; ok {
		if lastSeq < seq {
			delete(kv.clientMap, client)
			return false
		} else if lastSeq >= seq {
			return true
		}
	}
	return false
}

func (kv *KVServer) checkStore(key string) string {
	if val, ok := kv.store[key]; ok {
		return val
	}
	return ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if dupe := kv.isDuplicate(args.ClientId, args.Seq); !dupe {
		kv.clientMap[args.ClientId] = args.Seq

	}
	reply.Value = kv.checkStore(args.Key)

	delete(kv.appendRecords, args.ClientId)

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if dupe := kv.isDuplicate(args.ClientId, args.Seq); !dupe {
		kv.clientMap[args.ClientId] = args.Seq
		kv.store[args.Key] = args.Value
	}
	reply.Value = kv.store[args.Key]

	delete(kv.appendRecords, args.ClientId)

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if dupe := kv.isDuplicate(args.ClientId, args.Seq); dupe {
		reply.Value = kv.appendRecords[args.ClientId]
		return
	}
	old := kv.store[args.Key]
	reply.Value = old
	kv.appendRecords[args.ClientId] = old

	kv.store[args.Key] = old + args.Value
	kv.clientMap[args.ClientId] = args.Seq

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.clientMap = make(map[int64]int64)
	kv.appendRecords = make(map[int64]string)

	return kv
}
