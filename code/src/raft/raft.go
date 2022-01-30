/* 	Spring 2021
*	Khai Evdaev
*	CS451: Distributed Systems
*	Lab 2: Raft
 */

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* Persistent State on All Servers */
	currentTerm int         //current term server has seen
	votedFor    int         //candidate Id that received vote in current term
	log         []*LogEntry //log entries
	majority    int         //number of servers that make up the majority

	/* Volatile State on All Servers */
	commitIndex int  //index of highest log entry known to be committed
	lastApplied int  //index of highest log enry applied to state machine
	heartbeat   bool //a boolean that is set to true when a heartbeat is received and used by servers to initiate an election

	/*Volatile State on Leaders */
	nextIndex  []int //for each server, index of the next log entry TO SEND to that server
	matchIndex []int //for each server, index of the highest log entry KNOWN to be REPLICATED on server
	leader     bool

	applyCh chan ApplyMsg
}

/* Data Structure of Each Log Entry */
type LogEntry struct {
	Command interface{} //command for state machine
	Term    int         //term when the command was received
	Valid   bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = false
	if rf.leader == true {
		isleader = true
	}
	//fmt.Println("Server ", rf.me, " current term: ", term)
	//fmt.Println("Server ", rf.me, "am I the leader? ", isleader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		fmt.Println("Server ", rf.me, ": candidate's term - ", args.Term, " -- is less than mine -- ", rf.currentTerm)
		reply.VoteGranted = false
	} else if (rf.votedFor == 999 || rf.votedFor == args.CandidateId) && (len(rf.log) == 0 || (rf.log[len(rf.log)-1].Term <= args.LastLogTerm)) {

		if len(rf.log) != 0 && (rf.log[len(rf.log)-1].Term == args.LastLogTerm) {
			if len(rf.log)-1 <= args.LastLogIndex {
				//Server requesting vote does not have a higher term but I have not voted!
				reply.VoteGranted = true
				fmt.Println("Server ", rf.me, ": I have not voted and the candidate's log is up to date with mine")
				rf.votedFor = args.CandidateId
				rf.currentTerm = args.Term
				rf.leader = false
				rf.heartbeat = true //reset the timer when woken up
			} else {
				fmt.Println("Server ", rf.me, ": The candidate's log is shorter than mine!")
				reply.VoteGranted = false
			}
		} else {
			//Server requesting vote does not have a higher term but I have not voted!
			reply.VoteGranted = true
			fmt.Println("Server ", rf.me, ": I have not voted and the candidate's log is up to date with mine")
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.leader = false
			rf.heartbeat = true //reset the timer when woken up
		}
	} else {
		//Candidate's log is not up to date or I have already voted
		//fmt.Println("Server ", rf.me, ": The candidate's log is not up to date or I have already voted")
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	//?????: What to do with my current term?

}

type AppendEntriesArgs struct {
	Term         int         //leader's term
	LeaderId     int         //so follower can redirect clients
	PrevLogIndex int         //index of log entry immediately preceding new ones
	PrevLogTerm  int         //term of PrevLogIndex entry
	Entries      []*LogEntry //log entries to store (empty for heartbeat)
	LeaderCommit int         //leader's commit index

}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	Index   int  //in case there is a mismatch, follower sends last index of the conflicting term

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	fmt.Println("Server ", rf.me, " I received an Appendentries RPC from ", args.LeaderId)
	if (len(args.Entries) == 0 && rf.leader == false && (args.LeaderCommit >= rf.commitIndex)) || (len(args.Entries) == 0 && rf.leader == true && (args.Term >= rf.currentTerm)) {
		//Regular heartbeat for a follower
		fmt.Println("Server ", rf.me, " I got a heartbeat from Server: ", args.LeaderId)
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.heartbeat = true
		fmt.Println("Server ", rf.me, " set the heartbeat to ", rf.heartbeat)
		rf.leader = false

		if args.LeaderCommit > rf.commitIndex {

			fmt.Println("Server ", rf.me, ": leader has a higher commitindex than me!")
			fmt.Println("Server ", rf.me, " current lastApplied", rf.lastApplied)
			fmt.Println("Server ", rf.me, " current commitIndex", rf.commitIndex)

			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
			fmt.Println("Server ", rf.me, ": updated commitIndex to ", rf.commitIndex)
		}

		if rf.lastApplied < rf.commitIndex {
			for e := rf.lastApplied + 1; e < rf.commitIndex+1; e++ {
				apply_msg := ApplyMsg{}
				apply_msg.CommandValid = rf.log[e].Valid
				apply_msg.Command = rf.log[e].Command
				apply_msg.CommandIndex = e
				rf.lastApplied++
				rf.applyCh <- apply_msg
				fmt.Println("Server ", rf.me, ": sent an applyCh message with Command", apply_msg.Command, " and incremented lastApplied to ", rf.lastApplied)
			}
		}

	} else if args.Term < rf.currentTerm {
		fmt.Println("Server ", rf.me, " Got an append entries from outdated leader: ", args.LeaderId)
		fmt.Println("Server ", rf.me, "My term: ", rf.currentTerm, ". Leader's term: ", args.Term)
		fmt.Println("Server ", rf.me, "My commit index ", rf.commitIndex, " leader's commit index ", args.LeaderCommit)
		fmt.Println("Server ", rf.me, "Am I leader? ", rf.leader)
		fmt.Println("Server ", rf.me, "Length of entries is ", len(args.Entries))
		reply.Success = false
		reply.Term = rf.currentTerm

	} else if (args.Term >= rf.currentTerm) && (rf.leader == true) {
		//Append Entry from a new leader
		fmt.Println("Server ", rf.me, ": I got a heartbeat from a new leader! Reverting my leader status and becoming a follower")
		rf.leader = false
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.heartbeat = true
	} else if len(rf.log) == 0 && args.PrevLogTerm == 0 {
		fmt.Println("Server ", rf.me, ": received the dummy log entry!")

		rf.log = append(rf.log, args.Entries[0])
		reply.Success = true

	} else if (args.PrevLogIndex <= len(rf.log)-1) && (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		fmt.Println("Server ", rf.me, "The previous log entry mismatches with the term of the leader so sending false message!")
		fmt.Println("Server ", rf.me, ": leader PrevTerm ", args.PrevLogTerm, " and my prevTerm", rf.log[args.PrevLogIndex].Term)
		fmt.Println("Server ", rf.me, ": need to find the last mismatched index for that term")

		my_prev_term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term < my_prev_term {
				fmt.Println("Server ", rf.me, ": found the log entry with first occurance", rf.log[i].Term, "less than my prevTerm:  ", my_prev_term)
				fmt.Println("Server ", rf.me, " the index is ", i)
				reply.Index = i + 1
				break
			}
		}
		if my_prev_term == 0 || args.PrevLogIndex == 0 {
			fmt.Println("My prev term is 0 or PrevLogIndex is 0 so sending index 1")
			reply.Index = 1
		}
		reply.Index = args.PrevLogIndex
		reply.Success = false
		reply.Term = rf.currentTerm

	} else if args.PrevLogIndex > len(rf.log)-1 {
		reply.Index = len(rf.log)
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		fmt.Println("Server ", rf.me, ": received an AppendEntries from a legitimate leader with up-to-date logs!")
		fmt.Println("Server ", rf.me, ": The log entry is valid!")

		/* Step 1: Copy the new Entries Efficiently */
		// Create a new copy of the log from 0 to last matching index with the leader
		entry_ := rf.log[0 : args.PrevLogIndex+1]
		fmt.Println("Length of entry_ ", len(entry_))
		fmt.Println("Previndex: ", args.PrevLogIndex)
		fmt.Println("I have: ", rf.log[len(rf.log)-1].Term)

		for j := 0; j < len(args.Entries); j++ {
			entry_ = append(entry_, args.Entries[j]) //copy the subsequent entries
			fmt.Println("Server ", rf.me, "stored: ", entry_[j+1].Command, entry_[j+1].Term, " at index ")
		}
		rf.log = entry_ //renew the log stored
		reply.Success = true

		/* Step 2: Check which commands it we stored */
		for k := args.PrevLogIndex + 1; k < len(rf.log); k++ {
			fmt.Println("Server ", rf.me, "entry ", k, ": command ", rf.log[k].Command, " term ", rf.log[k].Term)
		}
		/* Step 3: Check whether to update the commit index */
		//Try to update the commit index
		if args.LeaderCommit > rf.commitIndex {

			if args.LeaderCommit < rf.lastApplied {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
			fmt.Println("Server ", rf.me, ": updated commitIndex to ", rf.commitIndex)

		}

		if rf.lastApplied < rf.commitIndex {
			for e := rf.lastApplied + 1; e < rf.commitIndex+1; e++ {
				apply_msg := ApplyMsg{}
				apply_msg.CommandValid = rf.log[e].Valid
				apply_msg.Command = rf.log[e].Command
				apply_msg.CommandIndex = e
				rf.lastApplied++
				rf.applyCh <- apply_msg
				fmt.Println("Server ", rf.me, ": sent an applyCh message with command", apply_msg.Command, " and incremented lastApplied to ", rf.lastApplied)
			}
		}
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("Need to send AppendEntries RPC to ", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.leader == true {
		// fmt.Println("Server ", rf.me, " I am beginning appendEntries!")
		// fmt.Println("Server ", rf.me, ": entry received from user -- ", command)

		/* Step 1:  Create a new log entry and store it in the log */
		//fmt.Println("Server ", rf.me, ": now initializing and storing the new command ", command)
		entry := LogEntry{}
		entry.Command = command
		entry.Term = rf.currentTerm
		entry.Valid = true
		rf.log = append(rf.log, &entry) //Append the entry
		//fmt.Println("Server ", rf.me, ": the size of log is now ", len(rf.log))

		/* Step 3: Update lastApplied, index, and term */
		index = len(rf.log) - 1 //Set the index to index of highest log entry applied (not committed)
		term = rf.currentTerm

		//fmt.Println("Server ", rf.me, "index and term sent to the tester: ", index, term)

		//Calling a function that will handle replication in order to return gracefully
		go func() {
			rf.log_replicate(&entry)
		}()

	} else {
		isLeader = false
	}

	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) log_replicate(entry *LogEntry) {

	//fmt.Println("Server ", rf.me, " need to replicate entry ", entry.Command, "with term: ", entry.Term)

	rf.mu.Lock()
	term := rf.currentTerm
	leader_id := rf.me
	prevEntry := len(rf.log) - 1       //prev entry index
	length_log := len(rf.log)          //length of the log
	prevTerm := rf.log[prevEntry].Term //term of the previous log entry
	entries := rf.log                  //copy entire entries
	leader_commit := rf.commitIndex    //commit index
	len := len(rf.peers)               //number of peers
	majority := rf.majority            //majority

	// Maybe also check whether I am the leader??

	rf.mu.Unlock()

	messages := make(chan string)

	/* Setting up the go routines to send AppendEntries to each server */
	for p := 0; p < len; p++ {

		if rf.me != p {
			go func(term int, leader_id int, prevEntry int, prevTerm int, entries []*LogEntry, leader_commit int, entry *LogEntry, majority int, peer int) {

				//index := prevEntry - 1
				done_ := false

				for done_ != true {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}

					args.Term = term          //Send my current Term
					args.LeaderId = leader_id //Send my leader ID
					rf.mu.Lock()
					args.PrevLogIndex = rf.nextIndex[peer] - 1 //Send the index of the previous log entry
					rf.mu.Unlock()
					if args.PrevLogIndex == -1 {
						args.PrevLogTerm = 0
						//args.PrevLogIndex = 0
					} else {
						args.PrevLogTerm = entries[args.PrevLogIndex].Term
					} //Send the term of the previous log entry
					entries_to_send := entries[args.PrevLogIndex+1:]
					args.Entries = entries_to_send //Set the entries list

					args.LeaderCommit = leader_commit

					reply.Index = -1 //needed later to see whether AppendEntries ever reached Server

					/*Print the data stored in args and reply for debugging */
					// fmt.Println("Server ", leader_id, ": trying to send the entry to server ", peer)
					// fmt.Println("Server ", leader_id, ": previous log index and term are ", args.PrevLogIndex, args.PrevLogTerm)
					// fmt.Println("Server ", leader_id, ": entries to send -- ", args.Entries)
					// fmt.Println("Server ", leader_id, ": commmit index: ", args.LeaderCommit)

					/* send an AppendEntries RPC */
					check := rf.sendAppendEntries(peer, &args, &reply)
					if check {
						fmt.Println("Server ", leader_id, " The RPC to ", peer, " was delivered!")
						if reply.Success {
							fmt.Println("Server ", leader_id, " : Server ", peer, " has replied successfully")
							rf.mu.Lock() /*Don't need */
							rf.nextIndex[peer]++
							rf.mu.Unlock()
							messages <- "success"
							done_ = true
						} else {
							fmt.Println("Server ", leader_id, " : Server ", peer, " did not reply successfully")
							fmt.Println("Server ", leader_id, " will try again")

							/* First check whether I am still a leader */
							rf.mu.Lock()
							leader_flag := rf.leader
							rf.mu.Unlock()
							if !leader_flag {
								fmt.Println("Server ", leader_id, " go routine: no longer leader so I will quit!")
								return
							}

							fmt.Println("Server ", leader_id, ": my term ", term, " term from AppendEntries reply: ", reply.Term)
							if reply.Term > term {
								fmt.Println("Server ", leader_id, ": I am an outdated leader, need to revert status and quit!")
								rf.mu.Lock()
								rf.leader = false
								rf.votedFor = 999
								messages <- "failure"
								rf.mu.Unlock()
								return
							}

							/* If I am still the leader, then have to retry but look at the conflicting index */
							//fmt.Println("Server ", leader_id, " the conflicting index: ", reply.Index)
							rf.mu.Lock()
							if reply.Index != -1 {
								rf.nextIndex[peer] = reply.Index
							}

							rf.mu.Unlock()
						}
					} else {

						// fmt.Println("Server ", leader_id, " The RPC to ",peer, " was not delivered!")
						// fmt.Println("Server ", leader_id, ": Check if I am a leader!")
						rf.mu.Lock()
						if rf.leader == false {
							//fmt.Println("Server ", leader_id, ": No longer a leader!")
							rf.mu.Unlock()
							done_ = true
							return
						}
						rf.mu.Unlock()

					}

				}

				/* Maybe use a Waitgroup to wait for a majority ? */

			}(term, leader_id, prevEntry, prevTerm, entries, leader_commit, entry, majority, p)
		}
	}

	/* Wait for the channel replies */
	// Important: begin counting from j = 1, since majority includes the leader too
	for j := 1; j < majority; j++ {
		//fmt.Println("Server ", leader_id, " waiting for a reply from appendEntries go routines")
		msg := <-messages
		fmt.Println("Server ", leader_id, " got reply: ", msg)
	}
	fmt.Println("Server ", leader_id, ": the majority have appended the new entry or I am no longer leader! ")
	fmt.Println("Server ", leader_id, ": need to update my commit index or revert leader status")

	rf.mu.Lock()
	if rf.leader == false {
		//fmt.Println("Server ", rf.me, ": no longer leader!")

		/* Need to delete old log entries? */
		entries_ := rf.log[0 : rf.commitIndex+1]
		rf.log = entries_
		fmt.Println("Updated log to previous. Basically, discard previous user entries!")
		rf.mu.Unlock()
		return
	}
	rf.commitIndex = length_log - 1
	fmt.Println("Server ", rf.me, ": updated commit index to ", rf.commitIndex)

	/* Now need to ApplyMsg */
	for j := rf.lastApplied + 1; j < rf.commitIndex+1; j++ {
		apply_mesg := ApplyMsg{}
		apply_mesg.CommandValid = rf.log[j].Valid
		apply_mesg.Command = rf.log[j].Command
		apply_mesg.CommandIndex = j
		rf.applyCh <- apply_mesg
		fmt.Println("Server ", rf.me, ": applyCh with command", apply_mesg.Command, " and index ", apply_mesg.CommandIndex)
		rf.lastApplied++
		fmt.Println("Server ", rf.me, ": updated lastApplied to ", rf.lastApplied)
	}
	rf.mu.Unlock()

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.majority = (int)(math.Ceil((float64(len(rf.peers)) / float64(2))))
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = 999 //999 signifies a null entity since golang does not have a NULL for ints
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.heartbeat = false
	finished := false
	rf.leader = false

	//Add a go routine to initialize an election
	go func() {

		//fmt.Println("Server ", rf.me, ": number of peers: ", len(rf.peers))
		//fmt.Println("Server ", rf.me, ": peers --", rf.peers)
		//for i:= 0; i < len(rf.peers); i++ {
		//fmt.Println("Server ", rf.me, " peer -- ", rf.peers[i])
		//}
		//fmt.Println("Server ", rf.me, ": majority: ", rf.majority )
		//fmt.Println("Server ", rf.me, ": initializing a timer" )

		for finished != true || rf.killed() != true {

			//Generating a generator and feeding it a seed that is non-constant
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			//The election timeout is currently 300-400
			t := (time.Duration)(300+r.Float64()*100) * time.Millisecond
			//fmt.Println("Server ", rf.me, ": going to sleep for ", t, " seconds")
			time.Sleep(t)
			fmt.Println("Server ", rf.me, ": woke up")

			rf.mu.Lock()
			//Case 1: Have not received any heartbeats and I am not a leader
			if rf.leader == false && rf.heartbeat == false && rf.killed() == false {
				fmt.Println("Server ", rf.me, ": did not receive a heartbeat!")
				fmt.Println("Server ", rf.me, ": need to start an election")

				//Need to send RequestVoteRPCs to all servers
				rf.currentTerm++    //increment my term
				rf.votedFor = rf.me //vote for myself
				//rf.mu.Unlock()

				go func(id int, term int, num_peers int) {

					//Set the initial count of votes to 1
					count := 1

					c1 := make(chan string, num_peers-1)

					for p := 0; p < num_peers; p++ {
						if p != id {
							go func(term_ int, id_ int, peer_id int) {

								//Create the RPC Arguments and Results
								args := RequestVoteArgs{}
								args.Term = term_
								args.CandidateId = id_
								rf.mu.Lock()
								if len(rf.log) != 0 {
									args.LastLogIndex = len(rf.log) - 1
									args.LastLogTerm = rf.log[args.LastLogIndex].Term
								} else {
									args.LastLogIndex = 0
									args.LastLogTerm = 0
								}
								rf.mu.Unlock()

								result := RequestVoteReply{}
								// Information about the RPC
								// fmt.Println("Server ", rf.me, " about to send an RPC")
								// fmt.Println("Server ", rf.me, " term: ", args.Term)
								// fmt.Println("Server ", rf.me, " candidateId: ", args.CandidateId)
								// fmt.Println("Server ", rf.me, " lastLogIndex: ", args.LastLogIndex)
								// fmt.Println("Server ", rf.me, " lastLogTerm: ", args.LastLogTerm)
								// fmt.Println("Server ", rf.me, " last log term in log[]: ", rf.log[args.LastLogIndex].Term)
								// fmt.Println("Server ", id, " sending a RequestElectionRPC to peer: ", peer_id)
								result_ := rf.sendRequestVote(peer_id, &args, &result)
								if result_ {
									if result.VoteGranted == true {
										c1 <- "success"
									}
								} else {
									c1 <- "failure"
								}

							}(term, id, p)

						}
					}

					//Now wait for the results
					for i := 0; i < num_peers-1; i++ {
						r0 := rand.New(rand.NewSource(time.Now().UnixNano()))
						rf.mu.Lock()
						if rf.votedFor != rf.me {
							rf.mu.Unlock()
							//fmt.Println("Server ", rf.me, ": Have already voted! No need to look at results from my requests!")
							return
						}
						rf.mu.Unlock()
						select {
						case res := <-c1:
							if res == "success" {
								//fmt.Println("Server ", id, " got a positive result")
								count++
							}

						case <-time.After((time.Duration)(r0.Float64()*10) * time.Millisecond):
							//fmt.Println("Server ", id, " timeout for the result from RequestVoteRPC")
						}
					}

					//Won Election!
					if count >= rf.majority {

						//fmt.Println("Server ", id, ": I have been elected a leader!")
						rf.mu.Lock()
						rf.leader = true
						//last_applied := rf.lastApplied + 1
						//fmt.Println("Server ", id, ": resetting all match indices to ", len(rf.log))
						for i := 0; i < len(rf.peers); i++ {
							rf.matchIndex = append(rf.matchIndex, 0) //Setting up these two arrays for the leader
							rf.nextIndex = append(rf.nextIndex, len(rf.log))
						}
						rf.mu.Unlock()
						//fmt.Println("Server ", id, ": need to send heartbeats now?!")

						//Need to send heartbeats periodically
						go func(term_ int, my_id_ int, num_peers int) {

							rf.mu.Lock()
							leader_flag := rf.leader
							fmt.Println("Server ", my_id_, ": should I send a heartbeat?")
							fmt.Println("Server ", my_id_, ": flag is ", leader_flag)
							rf.mu.Unlock()
							for leader_flag == true && rf.killed() == false {

								for p := 0; p < num_peers; p++ {

									if p != my_id_ {

										go func(id int, term int, peer int) {
											//Now need to send heartbeats
											args := AppendEntriesArgs{}
											args.Term = term
											args.LeaderId = id

											/* Need to include Commit Index with heartbeats */
											/* Is this a possible source of failure? */
											rf.mu.Lock()
											args.LeaderCommit = rf.commitIndex
											rf.mu.Unlock()

											reply := AppendEntriesReply{}
											//fmt.Println("Server ", my_id_, " sending a heartbeat to Server ", peer)
											rf.sendAppendEntries(peer, &args, &reply)
											fmt.Println("Server ", my_id_, " sent a heartbeat to Server ", peer)
											return
										}(my_id_, term_, p)
									}
								}

								//Now need to sleep
								r := rand.New(rand.NewSource(time.Now().UnixNano()))
								t := (time.Duration)(50+r.Float64()*20) * time.Millisecond
								//fmt.Println("Server ", my_id_, ": going to sleep for ", t, " seconds in the heartbeat go routine")
								time.Sleep(t)
								fmt.Println("Server ", my_id_, ": woke up in the heartbeat go routine")
								rf.mu.Lock()
								leader_flag = rf.leader
								fmt.Println("Server ", my_id_, ": am I leader? ", leader_flag)
								//sent_heartbeat = false
								rf.mu.Unlock()

							}
							//fmt.Println("Server ", my_id_, " I am no longer the leader!")
							rf.mu.Lock()
							rf.leader = false
							rf.mu.Unlock()
							return
						}(term, id, num_peers)

						rf.mu.Lock()
						if len(rf.log) == 0 {
							log0 := LogEntry{}
							log0.Term = 0
							log0.Valid = false
							rf.log = append(rf.log, &log0)
							//fmt.Println("Server ", rf.me, ": initialized the log with dummy entry, and the log length is - ", len(rf.log))
							//fmt.Println("Server ", rf.me, ": sent the dummy log entry for replication!")
							go func() {
								rf.log_replicate(&log0)
							}()
						}
						rf.mu.Unlock()
					} else { //Did not win the election so reset votedFor back to null (i.e. 999)
						//fmt.Println("Server ", rf.me, " did not win an election so changing votedFor back to 999")
						rf.mu.Lock()
						rf.votedFor = 999
						rf.mu.Unlock()
					}
					//rf.mu.Unlock()
				}(rf.me, rf.currentTerm, len(rf.peers))
				rf.mu.Unlock()

				//No need for an election because a heartbeat was received?
			} else if rf.leader == false && rf.heartbeat == true && rf.killed() == false {
				// fmt.Println("Server ", rf.me,": heartbeat received!")
				// fmt.Println("Server ", rf.me, ": voted for ", rf.votedFor)
				// fmt.Println("Server ", rf.me, ": current term ", rf.currentTerm)
				rf.heartbeat = false
				//rf.votedFor = 999			//set the votedFor back to null: we will switch it to back to leader's if a heartbeat is received
				rf.mu.Unlock() // if a heartbeat is not received, then it means that the state will move forward when we receive a new election request
			} else if rf.leader == true && rf.killed() == false {
				fmt.Println("Server ", rf.me, "I am still leader and I have not been killed!")
				rf.mu.Unlock()
				continue
			} else {
				finished = true
				rf.mu.Unlock()
				return
			}

		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
