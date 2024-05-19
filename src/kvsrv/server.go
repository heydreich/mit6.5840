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

	// Your definitions here.
	kvMap       map[string]string
	HasExecuted map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvMap[key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvMap[key] = value
	reply.Value = value
	// kv.HasExecuted[args.Token] = value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	var value = args.Value
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue := kv.kvMap[key]
	if oldval, ok := kv.HasExecuted[args.Token]; ok {
		reply.Value = oldval
		return
	}
	kv.HasExecuted[args.Token] = oldValue
	kv.kvMap[key] = oldValue + value
	reply.Value = oldValue
}

func (kv *KVServer) FinishPutAppend(args *FinishArgs, reply *FinishReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.HasExecuted, args.Token)

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.HasExecuted = make(map[int64]string)

	return kv
}
