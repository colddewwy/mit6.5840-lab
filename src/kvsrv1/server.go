package kvsrv

import (
	//"fmt"
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

type KVData struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	kvmap map[string]KVData
	// Your definitions here.

}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvmap = make(map[string]KVData)
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	data, ok := kv.kvmap[key]
	reply.Value = data.value
	reply.Version = data.version
	if ok {
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	data, ok := kv.kvmap[key]
	if ok {
		if args.Version == data.version {
			newdata := KVData{value: args.Value, version: (args.Version + 1)}
			kv.kvmap[key] = newdata
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == 0 {
			newdata := KVData{value: args.Value, version: (args.Version + 1)}
			kv.kvmap[key] = newdata
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
