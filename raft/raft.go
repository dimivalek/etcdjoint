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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
	pb "github.com/coreos/etcd/raft/raftpb"
)


// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateJointFollower StateType = iota
	StateCandidate
	StateJointCandidate
	StateLeader
	StateJointLeader
	StateLearner
	StateJointLearner
	StateLeavingLeader
	StatePreCandidate
	StateJointPreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
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

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64
// Added state types with prefix "joint". When a node becomes "joint", it means that system is under transient state and passes from one configuration to another
var stmap = [...]string{
	"StateFollower",
	"StateJointFollower",
	"StateLearner",
	"StateJointLearner",
	"StateCandidate",
	"StateJointCandidate",
	"StateLeader",
	"StateJointLeader",
	"StateLeavingLeader",
	"StatePreCandidate",
	"StateJointPreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
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

	// learners contains the IDs of all leaner nodes (including self if the local node is a leaner) in the raft cluster.
	// learners only receives entries from the leader node. It does not vote or promote itself.
	learners []uint64

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

	// MaxSizePerMsg limits the max size of each append message. Smaller value
	// lowers the raft recovery cost(initial probing and message lost during normal
	// operation). On the other side, it might affect the throughput during normal
	// replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
	// message.
	MaxSizePerMsg uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an innaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
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

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	id uint64

	Term uint64
	Vote uint64

	readStates []ReadState

	// the log
	raftLog *raftLog

	maxInflight 	int
	maxMsgSize  	uint64
	prs         	map[uint64]*Progress //member index progresses
	learnerPrs  	map[uint64]*Progress //learner index progresses
	newprs	    	map[uint64]*Progress //index progresses of the nodes of the new configuration
	oldprs	    	map[uint64]*Progress //index progresses of the nodes of the old configuration
	proposalindex 	uint64 //reconfiguration proposal index
	newconfindex  	uint64 //reconfiguration proposal index
	responses	[]uint64
	responsesold   	[]uint64 //C-old member responses with greater index than newconfindex
	responsesnew   	[]uint64 //C-new member responses with greater index than newconfindex
	hassend 	bool //checks whether the leader has send reconfiguration to nodes or not
	confids	    	[]uint64 //ids of the members of the new configuration
	updatedstructures bool //true if a node has updated the neccesary structs when passing to new configuration  
	state StateType
	firstcontactdone bool
	// isLearner is true if the local raft node is a learner.
	isLearner bool

	votes map[uint64]bool
	newvotes map[uint64]bool //votes that are necesarry in case of election when Cold and Cnew co-exist
	msgs []pb.Message

	// the leader id
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64
	// New configuration is ignored if there exists unapplied configuration.
	pendingConf bool

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	disableProposalForwarding bool

	tick func()
	step stepFunc

	logger Logger
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.Logger)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	learners := c.learners
	if len(cs.Nodes) > 0 || len(cs.Learners) > 0 {
		if len(peers) > 0 || len(learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		peers = cs.Nodes
		learners = cs.Learners
		
	}
	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 false,
		firstcontactdone:	   false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxInflight:               c.MaxInflightMsgs,
		prs:                       make(map[uint64]*Progress),
		learnerPrs:                make(map[uint64]*Progress),
		newprs:	    		   make(map[uint64]*Progress),
		oldprs:	    		   make(map[uint64]*Progress),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}
	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight)}
	}
	for _, p := range learners {
		if _, ok := r.prs[p]; ok {
			panic(fmt.Sprintf("node %x is in both learner and peer list", p))
		}
		r.learnerPrs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight), IsLearner: true}
		if r.id == p {
			r.isLearner = true
		}
	}
	
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	if !r.isLearner{
		r.becomeFollower(r.Term, None)
	}else { 
		r.becomeLearner(r.Term, None)
	}
	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}
///////////////////////////
func newRaft2(c *Config, islearner bool) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.Logger)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	learners := c.learners
	if len(cs.Nodes) > 0 || len(cs.Learners) > 0 {
		if len(peers) > 0 || len(learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		peers = cs.Nodes
		learners = cs.Learners
		
	}
	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 islearner,
		firstcontactdone:	   false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxInflight:               c.MaxInflightMsgs,
		prs:                       make(map[uint64]*Progress),
		learnerPrs:                make(map[uint64]*Progress),
		newprs:	    		   make(map[uint64]*Progress),
		oldprs:	    		   make(map[uint64]*Progress),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}
	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight)}
	}
	for _, p := range learners {
		if _, ok := r.prs[p]; ok {
			panic(fmt.Sprintf("node %x is in both learner and peer list", p))
		}
		r.learnerPrs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight), IsLearner: true}
		if r.id == p {
			r.isLearner = true
		}
	}
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	if !r.isLearner{
		r.becomeFollower(r.Term, None)
	}else { 
		r.becomeLearner(r.Term, None)
	}
	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) quorum() int { 
	return len(r.prs)/2 + 1 
}

func (r *raft) newquorum() int { 
	//check Cnew quorum
	return len(r.newprs)/2 + 1 
}

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs)+len(r.learnerPrs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	for id := range r.learnerPrs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

// send persists state to stable storage and then sends to its mailbox.
func (r *raft) send(m pb.Message) {
	m.From = r.id
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex && m.Type != pb.MsgPropRec {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
	
}

func (r *raft) getProgress(id uint64) *Progress {
	if pr, ok := r.prs[id]; ok {
		return pr
	}

	return r.learnerPrs[id]
}

// Append sends RPC, with entries to the given peer.
func (r *raft) sendAppend(to uint64) {
	pr := r.getProgress(to)
	if pr.IsPaused() {
		return
	}
	m := pb.Message{}
	m.To = to
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}

		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
}

func (r *raft) sendAppendRec(to uint64, confids []uint64, memberids []uint64, learnerids []uint64 ) {
//sends message MsgAppRec with extra field ConfIDs
	pr := r.getProgress(to)
	//return 
	if pr.IsPaused(){
		return
	}
	m := pb.Message{}
	m.To = to
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = pb.MsgAppRec
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.ConfIDs = confids
		m.Memberids = memberids
		m.Learnerids = learnerids 
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}

	}
	r.send(m)
}

func (r *raft) sendAppendFake(to uint64) int{
//actually does not send but used to check if all nodes are accesible to get reconfiguration message
	pr := r.getProgress(to)
	
	if pr==nil  {
		return 0
	} else if pr.IsPaused()  {
		return 0
	} 
	m := pb.Message{}
	m.To = to
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return 0
		}
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return 0
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
	} else {
		m.Type = pb.MsgAppRec
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		
		m.Commit = r.raftLog.committed
		return 1
		
	}
	return 1
}
func (r *raft) sendAppendRecfake(to uint64, confids []uint64) int{
//actually does not send but used to check if all nodes are accesible to get reconfiguration message
	pr := r.getProgress(to)
	
	if pr==nil{
		return 0
	} else if pr.IsPaused(){
		return 0
	}
	m := pb.Message{}
	m.To = to
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return 0
		}
		
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return 0
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		
	} else {
		m.Type = pb.MsgAppRec
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.ConfIDs = confids
		m.Commit = r.raftLog.committed
		return 1
	}
	return 1
}

func (r *raft) sendAppendNewConf(to uint64, confids []uint64) {
//sends MsgAppNewConf message
	pr := r.getProgress(to)
	if pr.IsPaused() {
		return
	}
	m := pb.Message{}
	m.To = to
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		m.Type = pb.MsgAppNewConf
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.ConfIDs = confids
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}

	}
	r.send(m)
}
// sendHeartbeat sends an empty MsgApp
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.getProgress(to).Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}
	r.send(m)	
}

func (r *raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.prs {
		f(id, pr)
	}

	for id, pr := range r.learnerPrs {
		f(id, pr)
	}
}

func (r *raft) forEachLearnerProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.learnerPrs {
		f(id, pr)
	}
}

func (r *raft) forEachMemberProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.prs {
		f(id, pr)
	}
}
func (r *raft) forEachProgressJoint(f func(id uint64, npr *Progress)) {
	for id, npr := range r.newprs {
		f(id, npr)
	}

}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	var futurenodes int
	var activenewquorum int
	r.forEachProgress(func(id uint64, _ *Progress) {
		activenewquorum+=r.sendAppendFake(id)
	})
	futurenodes = r.quorum()
	if activenewquorum < futurenodes {
		r.logger.Infof(" not ready to applyyyyyy, members are inactive ")
		return	
	} else {
		r.forEachProgress(func(id uint64, _ *Progress) {
			if id == r.id {
				return
			}

			r.sendAppend(id)
		})
	}
	
}

func (r *raft) bcastAppendRec(confids []uint64){
	var futurenodes int
	var activenewquorum int
	r.forEachProgressJoint(func(id uint64, _ *Progress) {
		activenewquorum+=r.sendAppendRecfake(id, confids)
	})
	futurenodes = len(confids)
	if activenewquorum < futurenodes {
		r.proposalindex = 0
		for _,id := range confids {
				delete(r.newprs,id)
		}
		r.logger.Infof(" not ready to reconfigure cluster, learners are inactive ")
		r.becomeLeader()
		return	
	} else {
		var memberids []uint64
		var learnerids []uint64
		r.forEachMemberProgress(func(id uint64, _ *Progress) {
			memberids = append(memberids, id)
		})
		r.forEachLearnerProgress(func(id uint64, _ *Progress) {
			learnerids = append(learnerids, id)
		})
		r.forEachProgress(func(id uint64, _ *Progress) {
			if id == r.id {
				return
			}
			r.sendAppendRec(id, confids, memberids, learnerids)
	
		})
	}
}

func (r *raft) bcastAppendNewConf(confids []uint64){
	
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendAppendNewConf(id, confids)
	})
}

func (r *raft) bcastAppendResp(){

	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
	
}
// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}
// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	// TODO(bmizerany): optimize.. Currently naive
	mis := make(uint64Slice, 0, len(r.prs))
	mil := make(uint64Slice, 0, len(r.learnerPrs))
	for _, p := range r.prs {
		mis = append(mis, p.Match)
	}
	for _, p := range r.learnerPrs {
		mil = append(mil, p.Match)
	}
	sort.Sort(sort.Reverse(mis))
	sort.Sort(sort.Reverse(mil))
	mci := mis[r.quorum()-1]
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) maybeCommitJoint(proposalindex uint64) bool {
	// when leader knows that system is in transient state, checks the progress not only for the existing members,but of the nodes of the new conf in order to increase commit index 
	mis := make(uint64Slice, 0, len(r.prs))
	mil := make(uint64Slice, 0, len(r.newprs))
	for _, p := range r.prs {
		mis = append(mis, p.Match)
	}
	for _, p := range r.newprs {
		mil = append(mil, p.Match)
	}
	sort.Sort(sort.Reverse(mis))
	sort.Sort(sort.Reverse(mil))
	mci := mis[r.quorum()-1]
	mli := mil[r.newquorum()-1]
	return r.raftLog.maybeCommitJoint(proposalindex, mci, mli, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	r.newvotes = make(map[uint64]bool)
	r.forEachProgress(func(id uint64, pr *Progress) {
		*pr = Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight), IsLearner: pr.IsLearner}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})
	r.forEachProgressJoint(func(id uint64, npr *Progress) {
		*npr = Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight), IsLearner: npr.IsLearner}
		if id == r.id {
			npr.Match = r.raftLog.lastIndex()
		}
	})
	r.pendingConf = false
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(r.raftLog.lastIndex())
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
}

func (r *raft) appendEntryJoint(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(r.raftLog.lastIndex())
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommitJoint(r.proposalindex)
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

func (r *raft) tickElectionJoint() {
	r.electionElapsed++
	if (r.promotableJoint() || r.promotable()) && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}
// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader && r.state != StateJointLeader{
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}
func (r *raft) becomeLearner(term uint64, lead uint64) {
	r.step = stepLearner
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateLearner
	r.logger.Infof("%x became learner at term %d", r.id, r.Term)
}
//learner unden joint consensus
func (r *raft) becomeJointLearner(term uint64, lead uint64) {
	r.step = stepLearner
	r.tick = r.tickElectionJoint
	r.lead = lead
	r.state = StateJointLearner
	r.logger.Infof("%x became joint learner at term %d", r.id, r.Term)
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}
//follower under joint consensus
func (r *raft) becomeJointFollower(term uint64, lead uint64) {
	r.state = StateJointFollower
	r.tick = r.tickElectionJoint
	r.lead = lead
	r.step = stepFollower
	r.isLearner = false
	r.logger.Infof("%x became joint follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader || r.state == StateJointLeader {
		panic("invalid transition [leader -> candidate]")
	}
	
	if r.state == StateJointFollower || r. state == StateJointPreCandidate {
		r.step = stepCandidate
		r.reset(r.Term + 1)
		r.tick = r.tickElectionJoint
		r.Vote = r.id
		r.state = StateJointCandidate
		r.logger.Infof("%x became joint candidate at term %d", r.id, r.Term)
	}else{
		r.step = stepCandidate
		r.reset(r.Term + 1)
		r.tick = r.tickElection
		r.Vote = r.id
		r.state = StateCandidate
		r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
	}
}

func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader || r.state == StateJointLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	r.step = stepCandidate
	r.votes = make(map[uint64]bool)
	r.newvotes = make(map[uint64]bool)
	
	if r.state == StateJointFollower{
		r.tick = r.tickElectionJoint
		r.state = StateJointPreCandidate
		r.logger.Infof("%x became joint pre-candidate at term %d", r.id, r.Term)
	}else{
		r.tick = r.tickElection
		r.state = StatePreCandidate
		r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
	}
}

func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.proposalindex = 0
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}

	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	r.appendEntry(pb.Entry{Data: nil})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}
//leader under joint consensus
func (r *raft) becomeJointLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower || r.state== StateJointFollower{
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.tick = r.tickHeartbeat
	r.state = StateJointLeader
	ents, err := r.raftLog.entries(r.raftLog.committed, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	r.appendEntry(pb.Entry{Data: nil})//maybe appendentry joint
	r.logger.Infof("%x became joint leader at term %d", r.id, r.Term)
}

func (r *raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
//if election happens under joint consensus, candidate has to receive majorities of votes of both of the configurations
	if r.state == StateJointCandidate || r.state == StateJointPreCandidate {
		if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) && r.newquorum() == r.newpoll(r.id, voteRespMsgType(voteMsg), true) {
			// We won the election after voting for ourselves (which must mean that
			// this is a single-node cluster). Advance to the next state.
			if t == campaignPreElection {
				r.campaign(campaignElection)
			} else {
				r.becomeJointLeader()
			}
			return
		}
	}else {
		if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) {
			// We won the election after voting for ourselves (which must mean that
			// this is a single-node cluster). Advance to the next state.
			if t == campaignPreElection {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
			}
			return
		}
	
	}
	if r.state == StateJointCandidate || r.state == StateJointPreCandidate {
		////by this way if the two sets have common servers, same messages will be sent more than once
		//// so it is possibly false logic
		for id := range r.prs {
				if id == r.id {
					continue
				}
				r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

				var ctx []byte
				if t == campaignTransfer {
					ctx = []byte(t)
				}
				r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
		}
		for id := range r.newprs {
			if id == r.id {
				continue
			}
			r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

			var ctx []byte
			if t == campaignTransfer {
				ctx = []byte(t)
			}
			r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
		}

	}else{
		for id := range r.prs {
			if id == r.id {
				continue
			}
			r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

			var ctx []byte
			if t == campaignTransfer {
				ctx = []byte(t)
			}
			r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
		}
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}
//count votes of new conf
func (r *raft) newpoll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	
	for id2 := range r.newprs {
		if id2 == id {
			if _, ok := r.newvotes[id]; !ok {
				r.newvotes[id] = v
			}
		}
	}
	for _, vv := range r.newvotes {
		if vv {
			granted++
		}
	}
	return granted
}
func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				///necessary to check joint states
				if r.isLearner && r.state == StateJointLearner{
					r.becomeJointLearner(m.Term, m.From)
				}else if r.isLearner && r.state == StateLearner{
					r.becomeLearner(m.Term, m.From)
				}else if !r.isLearner && r.state == StateJointFollower{
					r.becomeJointFollower(m.Term, m.From)
				}else if !r.isLearner && r.state == StateFollower{
					r.becomeFollower(m.Term, m.From)
				}else if r.state == StateJointCandidate{
					r.becomeJointFollower(m.Term, m.From)
				}else if r.state == StateCandidate{
					r.becomeFollower(m.Term, m.From)
				}else if r.state == StateJointLeader{
					r.becomeJointFollower(m.Term, m.From)
				}else if r.state == StateLeader{
					r.becomeFollower(m.Term, m.From)
				} else if r.state == StateLeavingLeader{
					r.becomeLearner(m.Term, m.From)				
				} else {
					r.becomeLearner(m.Term, m.From)				
				}
			} else if m.Type == pb.MsgAppRec || m.Type == pb.MsgAppNewConf{
				var isInNewConf bool
				for _,id := range m.ConfIDs{
					if id == r.id {
						isInNewConf = true
						r.becomeJointFollower(m.Term, m.From)
						break					
					}
				}
				if !isInNewConf {
					r.becomeJointLearner(m.Term, m.From)////may need modification 
				}
			} else {
				if r.isLearner && r.state == StateLearner{
					r.becomeLearner(m.Term, None)
				} else if r.isLearner && r.state == StateJointLearner{
					r.becomeJointLearner(m.Term, None)
				} else if !r.isLearner && r.state == StateFollower{
					r.becomeFollower(m.Term, None)
				} else if !r.isLearner && r.state == StateJointFollower {
					r.becomeJointFollower(m.Term, None)
				}
			}
			
		}

	case m.Term < r.Term:
		if r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		}else if r.checkQuorum && m.Type == pb.MsgAppRec {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			r.send(pb.Message{To: m.From, ConfIDs: m.ConfIDs, Type: pb.MsgAppRecResp})
		
		}else if r.checkQuorum && m.Type == pb.MsgAppNewConf {

			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			r.send(pb.Message{To: m.From, ConfIDs: m.ConfIDs, Type: pb.MsgAppNewConfResp})
		
		}else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		if r.state != StateLeader && r.state != StateJointLeader && r.state != StateLeavingLeader && r.state != StateLearner && r.state != StateJointLearner {
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		if r.isLearner {
			// TODO: learner may need to vote, in case of node down when confchange.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: learner can not vote",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			return nil
		}
		// The m.Term > r.Term clause is for MsgPreVote. For MsgVote m.Term should
		// always equal r.Term.
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why consider the
			// case where a single node was previously partitioned away and
			// it's local term is now of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		r.step(r, m)
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message)

func stepLeader(r *raft, m pb.Message) {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()//check leader state
		return
	case pb.MsgCheckQuorum:
		if  r.state == StateLeader{
			if !r.checkQuorumActive(){
				r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
				r.becomeFollower(r.Term, None)
			}
		} else if r.state == StateJointLeader{
			if !r.checkQuorumActiveJoint(){
				r.logger.Warningf("%x stepped down to joint follower since quorum is not active", r.id)
				r.becomeJointFollower(r.Term, None)
			}
		}
		return
	case pb.MsgProp:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if _, ok := r.prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return
		}

		for i, e := range m.Entries {
			if e.Type == pb.EntryConfChange{
				if r.pendingConf {
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration", e.String())
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				}
				r.pendingConf = true
			}
			
		}
		
		if r.state == StateJointLeader{
			r.appendEntryJoint(m.Entries...)
			r.bcastAppend()
		}else{
			r.appendEntry(m.Entries...)
			r.bcastAppend()
		}
		return
	case pb.MsgPropRec:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgPropRec", r.id)
		}
		if _, ok := r.prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return
		}
		if r.state == StateJointLeader{////maybe delete this
			if _, ok := r.newprs[r.id]; !ok {
				// If we are not currently a member of the range (i.e. this node
				// was removed from the configuration while serving as leader),
				// drop any new proposals.
				return
			}
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return
		}

		for i, e := range m.Entries {
			if e.Type == pb.EntryReConfChange {
				if r.pendingConf {
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration", e.String())
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else{
					for _,id := range m.ConfIDs{
						if r.prs[id] != nil {
							r.newprs[id] = r.prs[id]
						} 
						if r.learnerPrs[id] != nil {
							r.newprs[id] = r.learnerPrs[id]
						}
					}
					r.pendingConf = true
				}
			}
		}
		r.appendEntryJoint(m.Entries...)
		if r.proposalindex == 0{
					r.proposalindex = r.prs[r.id].Next - 2
		}
		//initialization of fields related to reconfiguration
		r.updatedstructures = false
		r.responsesold = nil
		r.responsesnew = nil
		r.confids = nil 
		r.hassend = false
		r.newconfindex = 0
		 
		for _,id := range m.ConfIDs{
			r.confids = append(r.confids, id) 		
		}
		r.becomeJointLeader()
		r.bcastAppendRec(m.ConfIDs)
		return
	case pb.MsgReadIndex:
		if r.quorum() > 1 {
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			switch r.readOnly.option {
			case ReadOnlySafe:
				r.readOnly.addRequest(r.raftLog.committed, m)
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed
				if m.From == None || m.From == r.id { // from local member
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {
			r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}

		return
	}

	// All other message types require a progress for m.From (pr).
	pr := r.getProgress(m.From)
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return
	}
	
	switch m.Type {
	case pb.MsgAppResp:
		var isInNewConf bool
		pr.RecentActive = true
		if m.Index > r.newconfindex && r.newconfindex > 0{
				for _,id := range r.confids{
					if r.responsesnew == nil && m.From == id{
						r.responsesnew = append(r.responsesnew, m.From)				
					}
					for i,id1 := range r.responsesnew{				
						if id1 == m.From {
							break						
						}
						if i == len(r.responsesnew)-1 && id1!=m.From{
							r.responsesnew = append(r.responsesnew, m.From)
						}	
					}
				}
					
		}
		if  len(r.responsesnew) >= r.quorum() && r.state == StateLeavingLeader{
				r.becomeLearner(r.Term,None)
				r.newconfindex = 0
		}
		if m.Reject {
			r.logger.Debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
					r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					pr.ins.freeTo(m.Index)
				}
				if r.state == StateLeavingLeader {
					if r.maybeCommit() {
						r.bcastAppend()
					} else if oldPaused {
							// update() reset the wait state on this node. If we had delayed sending
							// an update before, send it now.
						r.sendAppend(m.From)
					}
				}
				if r.state == StateJointLeader{
							mis := make(uint64Slice, 0, len(r.newprs))
							for _, p := range r.newprs {
								mis = append(mis, p.Match)
							}
							sort.Sort(sort.Reverse(mis))
							mci := mis[r.newquorum()-1]
							var bcastcheck bool 
							if r.proposalindex > mci {
								bcastcheck = false
							} else {
								bcastcheck = true
							}
							if r.maybeCommitJoint(r.proposalindex) {
									if r.hassend == false && m.Index > r.proposalindex+1 && bcastcheck == true{
										r.hassend = true
										r.newconfindex = r.prs[r.id].Next-2
										var allids []uint64
										for _,id := range r.confids{
											if r.prs[id] != nil {
												r.newprs[id] = r.prs[id]
											} 
											if r.learnerPrs[id] != nil {
												r.newprs[id] = r.learnerPrs[id]
											}
										}
										r.forEachProgress(func(id uint64, _ *Progress) {
											allids = append(allids, id)
										})
										for _,id := range allids{ 
											if r.prs[id] != nil { 
												r.oldprs[id]= r.prs[id]
											}
										}
										for _,id := range m.ConfIDs {
											if r.learnerPrs[id] != nil {
												r.learnerPrs[id].IsLearner = false 
											}
											if r.oldprs[id] != nil { 
													delete(r.oldprs,id)					
											}
										}
										for _,id := range r.confids {
											if r.prs[id] == nil{
												r.prs[id] = r.newprs[id]
												r.prs[id].IsLearner = false
												delete(r.learnerPrs, id)
											}
											if id == r.id {
												r.becomeLeader()
												isInNewConf = true
											}
											delete(r.newprs, id)
										}
										for _,id := range allids {
											for j,id2 := range r.confids {
												if id == id2 {
													break
												}
												if j+1 >= len(r.confids){
													if r.oldprs[id]!=nil{
														r.learnerPrs[id] = r.oldprs[id]
														r.learnerPrs[id].IsLearner = true
														delete(r.prs, id)
													}
					
												} 
											}		
										}
										if isInNewConf == false {
											r.state = StateLeavingLeader
											r.logger.Infof("%x became leaving leader at term %d", r.id, r.Term)
										}
										r.bcastAppendNewConf(r.confids)
									} else { 
										r.bcastAppend()
									}
							} else if oldPaused {
								// update() reset the wait state on this node. If we had delayed sending
								// an update before, send it now.
								r.sendAppend(m.From)
							}
					//}	
				}else{
					if r.maybeCommit() {
						r.bcastAppend()
					} else if oldPaused {
							// update() reset the wait state on this node. If we had delayed sending
							// an update before, send it now.
						r.sendAppend(m.From)
					}
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgAppRecResp:
		pr.RecentActive = true
		if r.state == StateLeavingLeader || r.state == StateLeader{
			return		
		}		
		var isInNewConf bool
		if m.Reject {
			r.logger.Debugf("%x received msgAppRec rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				var memberids []uint64
				var learnerids []uint64
				r.forEachMemberProgress(func(id uint64, _ *Progress) {
					memberids = append(memberids, id)
				})
				r.forEachLearnerProgress(func(id uint64, _ *Progress) {
					learnerids = append(learnerids, id)
				})
				r.sendAppendRec(m.From, m.ConfIDs, memberids, learnerids)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					pr.ins.freeTo(m.Index)
				}
					mis := make(uint64Slice, 0, len(r.newprs))
					for _, p := range r.newprs {
						mis = append(mis, p.Match)
					}
					sort.Sort(sort.Reverse(mis))
					mci := mis[r.newquorum()-1]
					var bcastcheck bool 
					if r.proposalindex > mci {
						bcastcheck = false
					} else {
						bcastcheck = true
					}
					if r.maybeCommitJoint(r.proposalindex){
						if r.hassend == true {
						} else if r.hassend == false && bcastcheck == true{
							r.hassend = true
							r.newconfindex = r.prs[r.id].Next-2
							var allids []uint64
							for _,id := range r.confids{
								if r.prs[id] != nil {
									r.newprs[id] = r.prs[id]
								} 
								if r.learnerPrs[id] != nil {
									r.newprs[id] = r.learnerPrs[id]
								}
							}
							r.forEachProgress(func(id uint64, _ *Progress) {
									allids = append(allids, id)
							})
							for _,id := range allids{ 
									if r.prs[id] != nil { 
										r.oldprs[id]= r.prs[id]
									}
							}
							for _,id := range m.ConfIDs {
								if r.learnerPrs[id] != nil {
									r.learnerPrs[id].IsLearner = false 
								}
								if r.oldprs[id] != nil { 
										delete(r.oldprs,id)					
								}
							}
							for _,id := range r.confids {
								if r.prs[id] == nil{
									r.prs[id] = r.newprs[id]
									r.prs[id].IsLearner = false
									delete(r.learnerPrs, id)
								}
								if id == r.id {
									r.becomeLeader()
									isInNewConf = true
								}
								delete(r.newprs, id)
							}
							for _,id := range allids {
								for j,id2 := range r.confids {
									if id == id2 {
										break
									}
									if j+1 >= len(r.confids){
										if r.oldprs[id]!=nil{
											r.learnerPrs[id] = r.oldprs[id]
											r.learnerPrs[id].IsLearner = true
											delete(r.prs, id)
										}
					
									} 
								}		
							}
							if isInNewConf == false {
								r.state = StateLeavingLeader
								r.logger.Infof("%x became leaving leader at term %d", r.id, r.Term)
							}
							r.bcastAppendNewConf(m.ConfIDs)
						}

					} else if oldPaused {
						// update() reset the wait state on this node. If we had delayed sending
						// an update before, send it now.
						var memberids []uint64
						var learnerids []uint64
						r.forEachMemberProgress(func(id uint64, _ *Progress) {
							memberids = append(memberids, id)
						})
						r.forEachLearnerProgress(func(id uint64, _ *Progress) {
							learnerids = append(learnerids, id)
						})
						r.sendAppendRec(m.From, m.ConfIDs, memberids, learnerids)
					}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppRecResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgAppNewConfResp:
		//var isInNewConf bool
		pr.RecentActive = true
		if r.state == StateLeader{
			return
		}
		for _,id := range r.confids{
			if r.responsesnew == nil && m.From == id{
				r.responsesnew = append(r.responsesnew, m.From)				
			}
			for i,id1 := range r.responsesnew{				
				if id1 == m.From {
					break						
				}
				if i == len(r.responsesnew)-1 && id1!=m.From{
					r.responsesnew = append(r.responsesnew, m.From)
				}	
			}
		}
		if m.Reject {
			r.logger.Debugf("%x received MsgAppNewConf rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				r.sendAppendNewConf(m.From, m.ConfIDs)
			}
		} else {
			oldPaused := pr.IsPaused()
			if r.newconfindex != 0 && len(r.responsesnew) >= r.quorum() && r.state == StateLeavingLeader{
					r.newconfindex = 0
					r.becomeLearner(r.Term, None)
			}
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					pr.ins.freeTo(m.Index)
				}
				r.pendingConf = false 
				if r.state == StateLeader{	
					if r.maybeCommit(){
						r.bcastAppend()
					}
				} else if r.state == StateJointLeader {
					if r.maybeCommitJoint(r.proposalindex){//maybecommit alteration
						r.bcastAppend()
					}				
				} else if r.state == StateLeavingLeader {
					if r.maybeCommit(){
						r.bcastAppend()					
					}
				} else if oldPaused {
					// update() reset the wait state on this node. If we had delayed sending
					// an update before, send it now.
					r.sendAppendNewConf(m.From, m.ConfIDs)
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppRecResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true

		pr.resume()
		//check states
		// free one slot for the full inflights window to allow progress.
		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return
		}
		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			return
		}

		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != ProgressStateSnapshot {
			return
		}
		if !m.Reject {
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the msgAppResp from the remote node before sending
		// out the next msgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.pause()
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate || r.state == StateJointPreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
	case pb.MsgPropRec:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
	case pb.MsgApp:
		if r.state== StateJointCandidate || r.state == StateJointPreCandidate || r.state == StateJointFollower{
			r.becomeJointFollower(r.Term, m.From)
			
		} else {
			r.becomeFollower(r.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MsgAppRec:
		if r.state== StateJointCandidate || r.state == StateJointPreCandidate || r.state == StateJointFollower{
			r.becomeJointFollower(r.Term, m.From)
			r.handleAppendEntriesRec(m)
		} else {
			r.becomeFollower(r.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MsgAppNewConf:
		if r.state== StateJointCandidate || r.state == StateJointPreCandidate || r.state == StateJointFollower{
			r.becomeJointFollower(r.Term, m.From)
			r.handleAppendEntriesNewConf(m)
		} else {
			r.becomeFollower(r.Term, m.From)
			r.handleAppendEntriesNewConf(m)
		}
	case pb.MsgHeartbeat:
		if r.state== StateJointCandidate || r.state == StateJointPreCandidate{
			r.becomeJointFollower(r.Term, m.From)
		} else {
			r.becomeFollower(r.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		if r.state== StateJointCandidate || r.state == StateJointPreCandidate{
			r.becomeJointFollower(r.Term, m.From)
		} else {
			r.becomeFollower(r.Term, m.From)
		}
		r.handleSnapshot(m)
	case myVoteRespType:
		if r.state== StateJointCandidate || r.state == StateJointPreCandidate {
			grn := r.newpoll(m.From, m.Type, !m.Reject)
			gr := r.poll(m.From, m.Type, !m.Reject)
			r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.newquorum(), grn, m.Type, len(r.newvotes)-grn)
			r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
			if gr > r.quorum() && grn > r.newquorum() {
				if r.state == StatePreCandidate {
					r.campaign(campaignElection)
				} else {
					r.becomeJointLeader()
					r.bcastAppendResp()
				}
			}
			if len(r.votes) - gr < 0 || len (r.newvotes) - grn < 0{
				r.becomeJointFollower(r.Term, None)
			}		
		}else{
			gr := r.poll(m.From, m.Type, !m.Reject)
			r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
			switch r.quorum() {
			case gr:
				if r.state == StatePreCandidate {
					r.campaign(campaignElection)
				} else {
					r.becomeLeader()
					r.bcastAppend()
				}
			case len(r.votes) - gr:
				r.becomeFollower(r.Term, None)
			}
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
}

func stepLearner(r *raft, m pb.Message){
switch m.Type {
	case pb.MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgPropRec:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
			r.lead = m.From
			r.electionElapsed = 0
			r.handleAppendEntries(m)
		
	case pb.MsgAppRec:
		if r.firstcontactdone == false {
			for _,id := range m.Memberids{
				if id == r.id {
					continue					
				}
				if r.prs[id]== nil{
					r.prs[id] = r.learnerPrs[id]
					delete(r.learnerPrs,id)					
				}
			}
			for _,id := range m.Learnerids{
				if id == r.id {
					continue					
				}
				if r.learnerPrs[id]== nil{
					r.learnerPrs[id] = r.prs[id]
					delete(r.prs,id)					
				}
			}
			r.firstcontactdone = true
		}
		if  (r.state != StateJointFollower || r.state != StateJointLearner){
			for _,id := range m.ConfIDs{
				if r.prs[id] != nil {
					r.newprs[id] = r.prs[id]
				}
				if r.learnerPrs[id] != nil {
					r.learnerPrs[id].IsLearner = false 
					r.newprs[id] = r.learnerPrs[id]
				}
			}
			for _,id := range m.ConfIDs{
				if r.id == id{
					r.isLearner = false
					r.electionElapsed = 0
					r.becomeJointFollower(r.Term, m.From)
					break
				}
			}
			if r.isLearner == true {
				r.electionElapsed = 0
				r.becomeJointLearner(r.Term, m.From)
			}
		}
		r.lead = m.From
		r.handleAppendEntriesRec(m)
	case pb.MsgAppNewConf:
		var allids []uint64
		if (r.state == StateJointFollower || r.state == StateJointLearner){
			for _,id := range m.ConfIDs{
				if r.newprs[id]==nil {
					if r.prs[id] != nil {
						r.newprs[id] = r.prs[id]
					}
					if r.learnerPrs[id] != nil {
						r.learnerPrs[id].IsLearner = false 
						r.newprs[id] = r.learnerPrs[id]
					}
					
				}
			}

			r.forEachProgress(func(id uint64, _ *Progress) {
				allids = append(allids, id)
			})
			for _,id := range allids{ 
				if r.prs[id] != nil { 
					r.oldprs[id]= r.prs[id]
				}
			}
			for _,id := range m.ConfIDs {		
				if r.prs[id] == nil{		
					r.prs[id] = r.newprs[id]
					delete(r.learnerPrs, id)		
				}
				delete(r.newprs, id)
				if r.oldprs[id] != nil { 
					delete(r.oldprs,id)					
				}
			
			}
				
			for _,id := range allids {
				for j,id2 := range m.ConfIDs {
					if id == id2 {
						break
					}
					if j+1 >= len(m.ConfIDs){
						if r.oldprs[id]!=nil{
							r.learnerPrs[id] = r.oldprs[id]
							r.learnerPrs[id].IsLearner = true
							delete(r.prs, id)
						}
					
					} 
				}		
			}	
		}
		r.lead = m.From
		r.becomeLearner(r.Term ,None)
		r.handleAppendEntriesNewConf(m)

	case pb.MsgHeartbeat:
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		return
	case pb.MsgTimeoutNow:
		return
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return
		}
		m.To = r.lead
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
}


func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case pb.MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgPropRec:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgAppRec:
		if r.firstcontactdone == false {
			for _,id := range m.Memberids{
				if id == r.id {
					continue					
				}
				if r.prs[id]== nil{
					r.prs[id] = r.learnerPrs[id]
					delete(r.learnerPrs,id)					
				}
			}
			for _,id := range m.Learnerids{
				if id == r.id {
					continue					
				}
				if r.learnerPrs[id]== nil{
					r.learnerPrs[id] = r.prs[id]
					delete(r.prs,id)					
				}
			}
			r.firstcontactdone = true
		}
		if r.state != StateJointFollower {
			for _,id := range m.ConfIDs{
				if r.prs[id] != nil {
					r.newprs[id] = r.prs[id]
				}
				if r.learnerPrs[id] != nil {
					r.learnerPrs[id].IsLearner = false 
					r.newprs[id] = r.learnerPrs[id]
					//delete(r.learnerPrs, id)
				}
			}	
			r.becomeJointFollower(r.Term, m.From)
		}
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntriesRec(m)
	case pb.MsgAppNewConf:
		var allids []uint64
		var isInNewConf bool
		if r.state == StateJointFollower {
			for _,id := range m.ConfIDs{
				if r.newprs[id]==nil {
					if r.prs[id] != nil {
						r.newprs[id] = r.prs[id]
					}
					if r.learnerPrs[id] != nil {
						r.learnerPrs[id].IsLearner = false 
						r.newprs[id] = r.learnerPrs[id]
					}
				}
			}
			r.forEachProgress(func(id uint64, _ *Progress) {
				allids = append(allids, id)
			})
			for _,id := range allids{ 
				if r.prs[id] != nil { 
					r.oldprs[id]= r.prs[id]
				}
			}
			for _,id := range m.ConfIDs{
				if id == r.id {
					r.state = StateFollower
					r.tick = r.tickElection
					isInNewConf = true
				}
				if r.prs[id] == nil{
					r.prs[id] = r.newprs[id]
					delete(r.learnerPrs, id)
				}
				delete(r.newprs, id)
				if r.oldprs[id] != nil { 
					delete(r.oldprs,id)					
				}
			}
			for _,id := range allids {
				for j,id2 := range m.ConfIDs {
					if id == id2 {
						break
					}
					if j+1 >= len(m.ConfIDs){
						if r.oldprs[id]!=nil{
							r.learnerPrs[id] = r.oldprs[id]
							r.learnerPrs[id].IsLearner = true
							delete(r.prs, id)
						}
					
					} 
				}
			}	
			if isInNewConf == false {
				r.isLearner = true
				r.becomeLearner(r.Term, m.From)
			} else {
			
				r.becomeFollower(r.Term, m.From)
			}
		}
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntriesNewConf(m)
	
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		if r.state == StateFollower{
			if r.promotable(){
				r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
				// Leadership transfers never use pre-vote even if r.preVote is true; we
				// know we are not recovering from a partition so there is no need for the
				// extra round trip.
				r.campaign(campaignTransfer)
			} else {
				r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
			}
		} else {
			if r.promotable() || r.promotableJoint() {
				r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
				// Leadership transfers never use pre-vote even if r.preVote is true; we
				// know we are not recovering from a partition so there is no need for the
				// extra round trip.
				r.campaign(campaignTransfer)
			} else {
				r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
			}
		
		}
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
}

func (r *raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleAppendEntriesRec(m pb.Message) {
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppRecResp, ConfIDs:m.ConfIDs, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppRecResp, ConfIDs:m.ConfIDs, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected msgAppRec [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppRecResp, ConfIDs:m.ConfIDs, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleAppendEntriesNewConf(m pb.Message) {
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppNewConfResp, ConfIDs:m.ConfIDs, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppNewConfResp, ConfIDs:m.ConfIDs, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgAppNewConfResp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppNewConfResp, ConfIDs:m.ConfIDs, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}


// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
func (r *raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}

	// The normal peer can't become learner.
	if !r.isLearner {
		for _, id := range s.Metadata.ConfState.Learners {
			if id == r.id {
				r.logger.Errorf("%x can't become learner when restores snapshot [index: %d, term: %d]", r.id, s.Metadata.Index, s.Metadata.Term)
				return false
			}
		}
	}

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)

	r.raftLog.restore(s)
	r.prs = make(map[uint64]*Progress)
	r.newprs = make(map[uint64]*Progress)
	r.learnerPrs = make(map[uint64]*Progress)
	r.restoreNode(s.Metadata.ConfState.Nodes, false)
	r.restoreNode(s.Metadata.ConfState.Learners, true)
	return true
}

func (r *raft) restoreNode(nodes []uint64, isLearner bool) {
	for _, n := range nodes {
		match, next := uint64(0), r.raftLog.lastIndex()+1
		if n == r.id {
			match = next - 1
			r.isLearner = isLearner
		}
		r.setProgress(n, match, next, isLearner)
		r.logger.Infof("%x restored progress of %x [%s]", r.id, n, r.getProgress(n))
	}
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	_, ok := r.prs[r.id]
	return ok
}

func (r *raft) promotableJoint() bool {
	_, ok := r.newprs[r.id]
	return ok
}

func (r *raft) addNode(id uint64) {
	r.addNodeOrLearnerNode(id, false)
}

func (r *raft) addLearner(id uint64) {
	r.addNodeOrLearnerNode(id, true)
}

func (r *raft) setNewConf(confids []uint64) {
	r.pendingConf = false
	for _,id := range confids{

		pr := r.getProgress(id)
		if pr == nil {
			r.setProgressnew(id, 0, r.raftLog.lastIndex()+1)
		}
		pr.RecentActive = true
	}

}

func (r *raft) addNodeOrLearnerNode(id uint64, isLearner bool) {
	r.pendingConf = false
	pr := r.getProgress(id)
	if pr == nil {
		r.setProgress(id, 0, r.raftLog.lastIndex()+1, isLearner)
	} else {
		if isLearner && !pr.IsLearner {
			// can only change Learner to Voter
			r.logger.Infof("%x ignored addLeaner: do not support changing %x from raft peer to learner.", r.id, id)
			return
		}

		if isLearner == pr.IsLearner {
			// Ignore any redundant addNode calls (which can happen because the
			// initial bootstrapping entries are applied twice).
			return
		}

		// change Learner to Voter, use origin Learner progress
		//delete(r.learnerPrs, id)
		//pr.IsLearner = false
		//r.prs[id] = pr
		
	}

	if r.id == id {
		r.isLearner = isLearner
	}

	// When a node is first added, we should mark it as recently active.
	// Otherwise, CheckQuorum may cause us to step down if it is invoked
	// before the added node has a chance to communicate with us.
	pr = r.getProgress(id)
	pr.RecentActive = true
}

func (r *raft) removeNode(id uint64) {
	r.delProgress(id)
	r.pendingConf = false

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.prs) == 0 && len(r.learnerPrs) == 0 {
		return
	}

	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.state == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
}

func (r *raft) resetPendingConf() { r.pendingConf = false }

func (r *raft) setProgress(id, match, next uint64, isLearner bool) {
	if !isLearner {
		delete(r.learnerPrs, id)
		r.prs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight)}
		return
	}

	if _, ok := r.prs[id]; ok {
		panic(fmt.Sprintf("%x unexpected changing from voter to learner for %x", r.id, id))
	}
	r.learnerPrs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight), IsLearner: true}
}

func (r *raft) setProgressnew(id, match, next uint64) {
	r.newprs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight)}
}

func (r *raft) delProgress(id uint64) {
	delete(r.prs, id)
	delete(r.learnerPrs, id)
	delete(r.newprs, id)
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// checkQuorumActive returns true if the quorum is active from
// the view of the local raft state machine. Otherwise, it returns
// false.
// checkQuorumActive also resets all RecentActive to false.
func (r *raft) checkQuorumActive() bool {
	var act int

	r.forEachProgress(func(id uint64, pr *Progress) {
		if id == r.id { // self is always active
			act++
			return
		}
		if pr.RecentActive && !pr.IsLearner {
			act++
		}

		pr.RecentActive = false
	})
	return act >= r.quorum()
}

func (r *raft) checkQuorumActiveJoint() bool {
	var act int
	var actnew int
	r.forEachProgress(func(id uint64, pr *Progress) {
		if id == r.id { // self is always active
			act++
			return
		}

		if pr.RecentActive && !pr.IsLearner {
			act++
		}

		pr.RecentActive = false
	})
		for i := range r.responses{
			actnew++
		}
	return (act >= r.quorum() && actnew >= r.newquorum())
}
func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
