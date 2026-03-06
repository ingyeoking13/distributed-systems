package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Data struct {
	Version rpc.Tversion
	Value   string
}

type KVServer struct {
	mu sync.Mutex
	kv map[string]Data
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kv = make(map[string]Data)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data, ok := kv.kv[args.Key]; !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.OK
		reply.Value = data.Value
		reply.Version = data.Version
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data, ok := kv.kv[args.Key]; !ok {
		if args.Version == 0 {
			kv.kv[args.Key] = Data{Version: 1, Value: args.Value}
			reply.Err = rpc.OK
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	} else if args.Version == data.Version {
		if args.Value == "del" {
			delete(kv.kv, args.Key)
			reply.Err = rpc.OK
			return
		}
		kv.kv[args.Key] = Data{Version: args.Version + 1, Value: args.Value}
		reply.Err = rpc.OK
		return
	}
	reply.Err = rpc.ErrVersion
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
