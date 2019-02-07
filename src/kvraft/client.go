package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers     []*labrpc.ClientEnd
	id          int64
	sequenceNum int
	leader      int
	mu          sync.Mutex
	// You will have to modify this struct.
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
	ck.id = nrand()
	ck.leader = 0
	return ck
}

func (ck *Clerk) CheckServerIsLeader(server *labrpc.ClientEnd) (int, bool) {
	args, reply := IsLeaderArgs{}, IsLeaderReply{}
	ok := server.Call("RaftKV.IsLeader", &args, &reply)
	if ok {
		return reply.Term, reply.IsLeader
	}

	return -1, false
}

func (ck *Clerk) ensureSingleLeader() bool {
	leadersByTerm := make(map[int][]int)

	for _, s := range ck.servers {
		term, isLeader := ck.CheckServerIsLeader(s)
		if term == -1 {
			// rpc call failed
			continue
		} else if isLeader {
			leadersByTerm[term] = append(leadersByTerm[term], 1)
		}
	}

	for term, lst := range leadersByTerm {
		if len(lst) > 1 {
			DPrintf("[LEADER VIOLATION] THERE IS MORE THAN ONE LEADER IN TERM %v", term)
			return false
		}
	}
	return true
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.id, ck.sequenceNum}
	reply := GetReply{}

	/*for {
		canProceed := ck.ensureSingleLeader()
		if canProceed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}*/

	cachedServer := ck.leader
	for reply.Err != OK {
		reply = GetReply{}
		ok := ck.servers[cachedServer%len(ck.servers)].Call("RaftKV.Get", &args, &reply)

		if ok {
			DPrintf("[client %v] Reply for get operation %v is: %+v", ck.id, args.Seq, reply)
			if reply.WrongLeader == true {
				cachedServer++
				continue
			}
			if reply.Err == ErrNoKey {
				ck.mu.Lock()
				ck.sequenceNum++
				ck.leader = cachedServer
				ck.mu.Unlock()
				return ""
			}
		} else {
			cachedServer++
		}
	}
	ck.mu.Lock()
	ck.leader = cachedServer
	ck.sequenceNum++
	ck.mu.Unlock()
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.id, ck.sequenceNum}
	reply := GetReply{}

	/*for {
		canProceed := ck.ensureSingleLeader()
		if canProceed {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}*/

	cachedServer := ck.leader
	for reply.Err != OK {
		ok := ck.servers[cachedServer%len(ck.servers)].Call("RaftKV.PutAppend", &args, &reply)

		if ok {
			if reply.WrongLeader == true {
				cachedServer++
				continue
			}
		} else {
			cachedServer++
		}
	}

	ck.mu.Lock()
	ck.leader = cachedServer
	ck.sequenceNum++
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
