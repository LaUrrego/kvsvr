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

type ClientRecord struct {
	LastSeq   int64
	LastValue string
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]string
	// keep track of clients and current sequence number
	clients map[int64]int64
	// keep track of clients and last reply sent
	clientMap map[int64]*ClientRecord
}

func (kv *KVServer) isDuplicate(client int64, seq int64) bool {
	if val, ok := kv.clients[client]; ok {
		return val >= seq
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if dupe := kv.isDuplicate(args.ClientId, args.Seq); dupe {
		previousReply := kv.clientMap[args.ClientId].LastValue
		reply.Value = previousReply
		return
	}
	if val, ok := kv.store[args.Key]; !ok {
		reply.Value = ""
	} else {
		reply.Value = val
	}

	kv.clientMap[args.ClientId] = &ClientRecord{LastSeq: args.Seq, LastValue: reply.Value}
	kv.clients[args.ClientId] = args.Seq
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if dupe := kv.isDuplicate(args.ClientId, args.Seq); dupe {
		previousReply := kv.clientMap[args.ClientId].LastValue
		reply.Value = previousReply
		return
	}
	kv.store[args.Key] = args.Value
	reply.Value = kv.store[args.Key]
	kv.clients[args.ClientId] = args.Seq
	kv.clientMap[args.ClientId] = &ClientRecord{LastSeq: args.Seq, LastValue: reply.Value}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if dupe := kv.isDuplicate(args.ClientId, args.Seq); dupe {
		previousReply := kv.clientMap[args.ClientId].LastValue
		reply.Value = previousReply
		return
	}
	old := kv.store[args.Key]
	if _, ok := kv.store[args.Key]; !ok {
		kv.store[args.Key] = args.Value
	} else {
		kv.store[args.Key] = old + args.Value
	}
	reply.Value = old
	kv.clients[args.ClientId] = args.Seq
	kv.clientMap[args.ClientId] = &ClientRecord{LastSeq: args.Seq, LastValue: reply.Value}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.clients = make(map[int64]int64)
	kv.clientMap = make(map[int64]*ClientRecord)

	return kv
}
