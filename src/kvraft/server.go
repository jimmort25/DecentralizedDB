package raftkv

import (
	"encoding/gob"
	"fmt"
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

type Result struct {
	Err   string
	Value string
	Index int
	Term  int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId   int
	CommandType string
	ClientId    int64
	Key         string
	Value       string
}

type RaftKV struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	currentTerm int
	lastIndex   int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db         map[string]string
	clientMap  map[int64]int
	agreeChans map[int]chan Result
}

func (kv *RaftKV) isDuplicate(client int64, seqNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, prs := kv.clientMap[client]

	if !prs || val < seqNum {
		return false
	}

	return true
}

func (kv *RaftKV) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	reply.Term, reply.IsLeader = kv.rf.GetState()
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	client := args.Ident
	command := args.Seq

	// Case 2: RaftKV Server is the Raft Leader

	agreementChannel := make(chan Result, 1)

	operation := Op{
		CommandId:   command,
		CommandType: "Get",
		ClientId:    client,
		Key:         args.Key,
	}

	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(operation)
	kv.agreeChans[index] = agreementChannel

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = Err(fmt.Sprintf(
			"[server %v] Not the leader", kv.me,
		))
		delete(kv.agreeChans, index)
		return
	}

	res := Result{}
	select {
	case <-time.After(time.Duration(1000) * time.Millisecond):
		DPrintf("[server %v] TIMED OUT: get request %v from client %v for key %v", kv.me, command, client, args.Key)
		kv.mu.Lock()
		delete(kv.agreeChans, index)
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	case res = <-agreementChannel:
		if res.Index == index && res.Term == term {
			kv.mu.Lock()
			kv.clientMap[client] = command
			val, prs := kv.db[args.Key]

			if prs == false {
				reply.Err = "ErrNoKey"
				reply.Value = ""
			} else {
				reply.Value = val
				reply.Err = OK
			}
			kv.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	reply.WrongLeader = false
	client := args.Ident
	command := args.Seq

	operation := Op{
		CommandId:   command,
		CommandType: args.Op,
		ClientId:    client,
		Key:         args.Key,
		Value:       args.Value,
	}

	kv.mu.Unlock()
	if kv.isDuplicate(client, command) {
		reply.Err = OK
		return
	}

	index, term, isLeader := kv.rf.Start(operation)
	kv.mu.Lock()
	agreementChannel := make(chan Result, 1)
	kv.agreeChans[index] = agreementChannel
	kv.mu.Unlock()

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = ""
		kv.mu.Lock()
		delete(kv.agreeChans, index)
		kv.mu.Unlock()
		return
	}

	res := Result{}
	select {
	case <-time.After(time.Duration(1000) * time.Millisecond):
		DPrintf(
			"[server %v] TIMED OUT: putappend request %v from client %v. Tried to put value %v to key %v",
			kv.me,
			command,
			client,
			args.Value,
			args.Key,
		)
		kv.mu.Lock()
		delete(kv.agreeChans, index)
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	case res = <-agreementChannel:
		if res.Term == term && res.Index == index {
			reply.Err = Err(res.Err)
		} else {
			reply.WrongLeader = true
		}
	}
	DPrintf(
		"[server %v] Executed putappend operation %v from client %v, key: %v, value: %v, operation type: %s",
		kv.me,
		command,
		client,
		args.Key,
		args.Value,
		args.Op,
	)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) runListener() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.Command == nil {
				continue
			}

			normalizeMsg := msg.Command.(Op)
			DPrintf("[server %v] Received index %v command: %+v", kv.me, msg.Index, normalizeMsg)

			res := Result{}
			res.Term, res.Index = msg.Term, msg.Index

			if kv.isDuplicate(normalizeMsg.ClientId, normalizeMsg.CommandId) == false {
				kv.mu.Lock()
				switch normalizeMsg.CommandType {
				case "Put":
					kv.clientMap[normalizeMsg.ClientId] = normalizeMsg.CommandId
					kv.db[normalizeMsg.Key] = normalizeMsg.Value
					res.Err = OK
				case "Append":
					kv.clientMap[normalizeMsg.ClientId] = normalizeMsg.CommandId
					_, prs := kv.db[normalizeMsg.Key]
					if prs {
						kv.db[normalizeMsg.Key] += normalizeMsg.Value
					} else {
						kv.db[normalizeMsg.Key] = normalizeMsg.Value
					}
					res.Err = OK
				}
				kv.mu.Unlock()
			} else {
				DPrintf("[server %v] <DUPLICATE> Refused index %v command: %+v", kv.me, msg.Index, normalizeMsg)
				res.Err = OK
			}

			kv.mu.Lock()
			ch, ok := kv.agreeChans[msg.Index]
			if ok {
				delete(kv.agreeChans, msg.Index)
				ch <- res
			}
			kv.mu.Unlock()

			DPrintf("[server %v] After executing, the database looks like: %+v\n", kv.me, kv.db)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.clientMap = make(map[int64]int)
	kv.db = make(map[string]string)
	kv.agreeChans = make(map[int]chan Result)

	go kv.runListener()

	return kv
}
