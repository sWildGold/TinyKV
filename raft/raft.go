// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sync"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

type stepFunc func(r *Raft, m pb.Message) error

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election timeout
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	tick func()

	step stepFunc

	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, _ := c.Storage.InitialState()

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              nil,
		State:            StateFollower,
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		peers:            c.peers,
	}
	r.becomeFollower(0, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tickElection() {
	// Your Code Here (2A).
	r.electionElapsed++

	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Debugf("tickHeartbeat error")
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.tick = r.tickElection
	r.step = stepFollower
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.State = StateFollower
	r.resetRandomizedElectionTimeOut()
	log.Debugf("[id:%x,term:%x]becomeFollower", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.step = stepCandidate
	r.tick = r.tickElection
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.State = StateCandidate
	r.Lead = None
	r.resetRandomizedElectionTimeOut()
	r.votes = make(map[uint64]bool)
	for _, id := range r.peers {
		if r.id == id {
			r.votes[id] = true
		} else {
			r.votes[id] = false
		}
	}
	log.Debugf("[id:%x,term:%x] becomeCandidate", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.step = stepLeader
	r.tick = r.tickHeartbeat
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Prs = make(map[uint64]*Progress)
	for _, id := range r.peers {
		r.Prs[id] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
	}
	log.Debugf("[id:%x,term:%x] becomeLeader", r.id, r.Term)
	//append noop entry to itself
	//emptyEnt := pb.Entry{Data: nil}
	//r.appendEntry(emptyEnt)
	//r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bcastHeartBeat()
		} else {
			return errors.New("raft state is not leader, cannot receive MsgBeat")
		}
	default:
		if err := r.step(r, m); err != nil {
			return err
		}
	}
	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppendResponse:

	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.Term == m.Term || m.Term == 0 {
			r.campaign()
		}
	case pb.MessageType_MsgRequestVoteResponse:
		r.receiveVote(m)
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.Term == m.Term || m.Term == 0 {
			r.campaign()
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	for _, id := range r.peers {
		if r.id == id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastTerm(),
		}
		r.send(m)
	}

	//single node
	if len(r.peers) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) resetRandomizedElectionTimeOut() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg_resonse := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
	}
	defer r.send(msg_resonse)
	if m.Term < r.Term {
		msg_resonse.Term = r.Term
		msg_resonse.Reject = false
		log.Debugf("[id:%x,term:%x] receive append reject (small term) from [id:%x,term:%x]",
			r.id, r.Term, m.From, m.Term)
		return
	}
	r.becomeFollower(m.Term, m.From)
	if term, err := r.RaftLog.Term(m.Index); err != nil || term != m.LogTerm {
		msg_resonse.Reject = true
		msg_resonse.Term = r.Term
		log.Debugf("[id:%x,term:%x] receive append reject (log mismatch) from [id:%x,term:%x]",
			r.id, r.Term, m.From, m.Term)
		return
	}
	var i uint64 = 0
	for int(i) < len(m.Entries) {
		term, err := r.RaftLog.Term(m.Index + i)
		if err != nil || term != m.Entries[int(i)].Term {
			i--
			break
		}
		i++
	}
	r.RaftLog.entries = append(make([]pb.Entry, 0), r.RaftLog.entries[:i+m.Index-r.RaftLog.stabled]...)
	i++
	for int(i) < len(m.Entries) {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[int(i)])
		i++
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = m.Commit
	}
	msg_resonse.Reject = false
	msg_resonse.Term = r.Term
	log.Debugf("[id:%x,term:%x] receive append accept from [id:%x,term:%x]",
		r.id, r.Term, m.From, m.Term)

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg_response := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
		msg_response.Reject = false
		log.Debugf("[id:%x,term:%x] receive heartbeat agree from [id:%x,term:%x]",
			r.id, r.Term, m.From, m.Term)
	} else {
		msg_response.Reject = true
		log.Debugf("[id:%x,term:%x] receive heartbeat reject from [id:%x,term:%x]",
			r.id, r.Term, m.From, m.Term)
	}
	msg_response.Term = r.Term
	r.send(msg_response)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	msg_response := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Term > m.Term || (r.Vote != m.From && r.Vote != None) || !r.isAtLeastUpToDate(m.Index, m.LogTerm) {
		msg_response.Reject = true
		msg_response.Term = r.Term
		r.send(msg_response)
		log.Debugf("[id:%x,term:%x,Index:%x,LogTerm:%x] reject RequestVote from [id:%x,term:%x,Index:%x,LogTerm:%x]",
			r.id, r.Term, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), m.From, m.Term, m.Index, m.LogTerm)
		return
	}
	msg_response.Reject = false
	msg_response.Term = r.Term
	r.Vote = m.From
	log.Debugf("[id:%x,term:%x,Index:%x,LogTerm:%x] agree RequestVote from [id:%x,term:%x,Index:%x,LogTerm:%x]",
		r.id, r.Term, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), m.From, m.Term, m.Index, m.LogTerm)
	r.send(msg_response)
}

func (r *Raft) receiveVote(m pb.Message) {
	if m.Term < r.Term || m.Reject {
		return
	}
	if m.Term > r.Term {
		panic("should not receive this Vote")
	}
	r.votes[m.From] = true
	voteCount := 0
	for _, value := range r.votes {
		if value {
			voteCount++
		}
	}
	if 2*voteCount > len(r.peers) {
		r.becomeLeader()
	}
}

func (r *Raft) receiveAppendResp(m pb.Message) {

}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// isAtLeastUpToDate return if candidate is at least up-to-date as itself.
func (r *Raft) isAtLeastUpToDate(candidateIndex, candidateLogTerm uint64) bool {
	if r.RaftLog.LastTerm() > candidateLogTerm {
		return false
	} else if r.RaftLog.LastTerm() < candidateLogTerm {
		return true
	}
	if candidateIndex >= r.RaftLog.LastIndex() {
		return true
	}
	return false
}
func (r *Raft) bcastHeartBeat() {
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
		}
		r.send(m)
	}
}
func (r *Raft) bcastAppend() {
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		prevLogIndex := r.Prs[id].Next - 1
		prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
		if err != nil {
			panic("bcastAppend should not exceed")
		}
		entries := make([]*pb.Entry, 0)
		for i := prevLogIndex + 1; i < r.RaftLog.LastIndex()+1; i++ {
			if i >= r.RaftLog.stabled+uint64(len(r.RaftLog.entries)) {
				break
			}
			entries = append(entries, &r.RaftLog.entries[i])
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			LogTerm: prevLogTerm,
			Index:   prevLogIndex,
			Entries: entries,
		}
		r.send(m)
	}
}

func (r *Raft) appendEntry(ents ...pb.Entry) {
	r.RaftLog.append(ents...)
	log.Debugf("[id:%x,term:%x] appendEntry", r.id, r.Term)
}
