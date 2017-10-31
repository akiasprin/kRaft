package pink

import (
	"fmt"
	"net"
	"pink/protodef"
	"sync"
	"time"

	context "golang.org/x/net/context"

	"github.com/ender-wan/ewlog"

	"google.golang.org/grpc"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
	CLOSED
	NULLVOTE = -1
)

type ApplyMsg struct {
	Index   uint64
	Command interface{}
}

type Server struct {
	mu    sync.Mutex
	peers []string
	me    int

	CurrentTerm uint64
	VotedFor    int
	Logs        []protodef.LogEntry
	CommitIndex uint64
	LastApplied uint64
	NextIndex   []uint64
	MatchIndex  []uint64

	voteCount int
	state     int

	recvHeartbeatChan chan bool
	winElectionChan   chan bool
	commitChan        chan bool
	applyChan         chan *ApplyMsg

	clientRequest map[uint64]bool
	logsSequence  map[uint64]uint64

	rpc      *grpc.Server
	port     int
	isClosed bool
}

func (s *Server) SetState(code int) {
	s.state = code
}

func (s *Server) GetState() (uint64, bool, int) {
	return s.CurrentTerm, s.state == LEADER, s.state
}

func (s *Server) getLastTerm() uint64 {
	return s.Logs[len(s.Logs)-1].Term
}

func (s *Server) getLastIndex() uint64 {
	return s.Logs[(len(s.Logs))-1].Index
}

// Retrieves the number of servers required to make a quorum.
func (s *Server) QuorumSize() int {
	return (len(s.peers) / 2) + 1
}

// RequestVote RPC Handler.
func (s *Server) RequestVote(context context.Context, in *protodef.VoteRequest) (
	out *protodef.VoteResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out = &protodef.VoteResponse{}
	out.VoteGranted = false

	// Reject a former term
	if in.Term < s.CurrentTerm {
		out.Term = s.CurrentTerm
		return
	}

	if in.Term > s.CurrentTerm {
		s.CurrentTerm = in.Term
		s.VotedFor = NULLVOTE
		s.SetState(FOLLOWER)
	}

	canVote := s.VotedFor == NULLVOTE || s.VotedFor == int(in.CandidateID)
	// Test if satify election restriction
	canUpdateLogs := in.LastLogTerm > s.getLastTerm() ||
		(in.LastLogTerm == s.getLastTerm() && in.LastLogIndex >= s.getLastIndex())

	if canVote && canUpdateLogs {
		s.VotedFor = int(in.CandidateID)
		out.VoteGranted = true
		s.SetState(FOLLOWER)
	}

	out.Term = s.CurrentTerm

	return
}

// AppendEntries RPC Handler.
func (s *Server) AppendEntries(context context.Context, in *protodef.AppendEntriesRequest) (
	out *protodef.AppendEntriesResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ewlog.Debug(in)

	out = &protodef.AppendEntriesResponse{}
	out.Success = false
	out.NextIndex = s.getLastIndex() + 1

	// Reject a former term.
	if in.Term < s.CurrentTerm {
		out.Term = s.CurrentTerm
		return
	}

	// Cluster has existed a leader.
	s.recvHeartbeatChan <- true

	if in.Term > s.CurrentTerm {
		s.CurrentTerm = in.Term
		s.SetState(FOLLOWER)
		s.VotedFor = NULLVOTE
	}

	out.Term = s.CurrentTerm

	// Reject lack of previous logs.
	if in.PrevLogIndex > s.getLastIndex() {
		return
	}

	// Retrieves the start postion of inconsistent logs.
	localPrevLogTerm := s.Logs[in.PrevLogIndex].Term
	if in.PrevLogTerm != localPrevLogTerm {
		for i := in.PrevLogIndex - 1; i >= 0; i-- {
			if s.Logs[i].Term != localPrevLogTerm {
				out.NextIndex = i + 1
				break
			}
		}
		return
	}

	tmp := []protodef.LogEntry{}
	for _, entry := range in.Entries {
		tmp = append(tmp, *entry)
	}
	s.Logs = append(s.Logs[:in.PrevLogIndex+1], tmp...)

	for i := 0; i <= len(tmp); i++ {
		s.logsSequence[in.PrevLogIndex+1+uint64(i)] = 0
	}

	out.Success = true
	// out.NextIndex = s.getLastIndex()

	// Leader has confirmed new commits
	if in.LeaderCommit > s.CommitIndex {
		s.CommitIndex = getMin(in.LeaderCommit, s.getLastIndex())
		s.commitChan <- true
	}

	return
}

func (s *Server) updateCommits() {
	furthestCommit := s.CommitIndex
	for i := furthestCommit + 1; i <= uint64(len(s.Logs)-1); i++ {
		replicationCount := 1 // start count at 1 for ourselves
		for peer := range s.peers {
			if peer != s.me && s.MatchIndex[peer] >= i && s.Logs[i].Term == s.CurrentTerm {
				replicationCount++
			}
		}
		if replicationCount >= s.QuorumSize() {
			furthestCommit = i
		}
	}
	didCommit := furthestCommit > s.CommitIndex
	s.CommitIndex = furthestCommit
	ewlog.Debugf("s.CommitIndex:%v, s.MatchIndex:%+v\n", s.CommitIndex, s.MatchIndex)
	if didCommit {
		s.commitChan <- true
	}
}

func (s *Server) broadcast() {
	if s.state != LEADER {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCommits()
	for idx, peer := range s.peers {
		if idx != s.me {
			prevLogIndex := getMin(s.NextIndex[idx]-1, s.getLastIndex())
			args := &protodef.AppendEntriesRequest{
				Term:         s.CurrentTerm,
				LeaderID:     int32(s.me),
				LeaderCommit: s.CommitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  s.Logs[prevLogIndex].Term,
				Entries:      make([]*protodef.LogEntry, 0),
			}
			for i := args.PrevLogIndex + 1; i < uint64(len(s.Logs)); i++ {
				args.Entries = append(args.Entries, &s.Logs[i])
			}

			ewlog.Debugf("%v->%v: Broadcast AppendEntries: %v\n", s.me, idx, args.Entries)

			go func(idx int, peer string) {
				conn, err := grpc.Dial(peer, []grpc.DialOption{grpc.WithTimeout(30 * time.Millisecond), grpc.WithInsecure()}...)
				if err != nil {
					// ewlog.Infof("Host[%v] gRPC error: %v\n", idx, err)
					return
				}
				defer conn.Close()
				c := protodef.NewRaftClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				reply, err := c.AppendEntries(ctx, args) // Call Remote Server.AppendEntries().
				if err != nil {
					// ewlog.Infof("Host[%v] gRPC error: %v\n", idx, err)
					return
				}

				s.mu.Lock()
				if reply.Term > s.CurrentTerm {
					s.CurrentTerm = reply.Term
					s.VotedFor = NULLVOTE
					s.SetState(FOLLOWER)
				}
				// Store the start postion of the follower's logs.
				nEntries := len(args.Entries)
				if reply.Success && nEntries > 0 {
					s.NextIndex[idx] = reply.NextIndex
					s.MatchIndex[idx] = s.NextIndex[idx] - 1
				} else if !reply.Success {
					s.NextIndex[idx] = reply.NextIndex
				}
				s.mu.Unlock()

				ewlog.Debugf("%v->%v relpy nextIndex:%v\n", idx, s.me, reply.NextIndex)

			}(idx, peer)
		}
	}
}

func (s *Server) holdElection() {
	s.updateElectionState()
	args := &protodef.VoteRequest{
		Term:         s.CurrentTerm,
		CandidateID:  int32(s.me),
		LastLogIndex: s.getLastIndex(),
		LastLogTerm:  s.getLastTerm(),
	}
	for idx, peer := range s.peers {
		if idx != s.me {
			go func(idx int, peer string) {
				conn, err := grpc.Dial(peer, []grpc.DialOption{grpc.WithTimeout(30 * time.Millisecond), grpc.WithInsecure()}...)
				if err != nil {
					// ewlog.Infof("Host[%v] gRPC error: %v\n", idx, err)
					return
				}
				defer conn.Close()
				c := protodef.NewRaftClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				reply, err := c.RequestVote(ctx, args) // Call Remote Server.RequestVote().
				if err != nil {
					// ewlog.Infof("host[%v] gRPC error: %v\n", idx, err)
					return
				}

				s.mu.Lock()
				if reply.Term > s.CurrentTerm {
					s.CurrentTerm = reply.Term
					s.VotedFor = NULLVOTE
					s.SetState(FOLLOWER)
				}
				s.mu.Unlock()

				if reply.VoteGranted {
					s.mu.Lock()
					s.voteCount++
					s.mu.Unlock()

					if s.voteCount >= s.QuorumSize() {
						s.winElectionChan <- true
					}
				}
			}(idx, peer)
		}
	}
}

func (s *Server) updateElectionState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentTerm++
	s.voteCount = 1
	s.VotedFor = s.me
	s.SetState(CANDIDATE)
}

func (s *Server) demoteToFollower() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SetState(FOLLOWER)
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	s.SetState(LEADER)
	nextIndex := s.getLastIndex() + 1
	for i := range s.peers {
		s.NextIndex[i] = nextIndex
		s.MatchIndex[i] = 0
	}
	s.mu.Unlock()

	ewlog.Infof("New Leader: %+v, VoteCount:%+v\n", s.me, s.voteCount)

	s.broadcast()
}

func (s *Server) committer() {
	for {
		select {
		case state := <-s.commitChan:
			if state {
				s.mu.Lock()
				for i := s.LastApplied + 1; i <= s.CommitIndex; i++ {
					msg := &ApplyMsg{Index: i, Command: s.Logs[i].Command}
					s.applyChan <- msg // applyChan should be a no cache channel.
					s.LastApplied = i
				}
				s.mu.Unlock()
			} else {
				return
			}
		}
	}
	ewlog.Info("stop commiter...")
}

func (s *Server) loop() {
	for {
		if !s.isClosed {
			switch s.state {
			case FOLLOWER:
				select {
				case <-s.recvHeartbeatChan:
				case <-time.After(electionTimeout()):
					s.SetState(CANDIDATE)
				}
			case LEADER:
				select {
				case <-time.After(HeartbeatTimeout):
					s.broadcast()
				}
			case CANDIDATE:
				s.holdElection()
				select {
				case <-time.After(electionTimeout()):
				case <-s.recvHeartbeatChan:
					s.demoteToFollower()
				case <-s.winElectionChan:
					s.becomeLeader()
				}
			}
		} else {
			s.SetState(CLOSED)
			s.rpc.GracefulStop()
			s.commitChan <- false
			ewlog.Infof("ID: %v CLOSED.\n", s.me)
			return
		}
	}
}

func (s *Server) Close() {
	s.isClosed = true
}

func (s *Server) Start() (err error) {
	s.SetState(FOLLOWER)
	s.isClosed = false
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return
	}
	s.rpc = grpc.NewServer()
	protodef.RegisterRaftServer(s.rpc, s)
	go s.rpc.Serve(listen)
	go s.committer()
	go s.loop()
	return
}

func NewRaftServer(peers []string, me int, port int, applyChan chan *ApplyMsg) (s *Server, err error) {
	s = &Server{}
	s.peers = peers
	s.me = me
	s.VotedFor = NULLVOTE
	s.CurrentTerm = 0
	s.Logs = []protodef.LogEntry{protodef.LogEntry{Term: 0}}
	s.NextIndex = make([]uint64, len(s.peers))
	s.MatchIndex = make([]uint64, len(s.peers))
	s.recvHeartbeatChan = make(chan bool, len(peers))
	s.winElectionChan = make(chan bool, len(peers))
	s.commitChan = make(chan bool)
	s.applyChan = applyChan
	s.port = port
	s.CommitIndex = 0
	s.clientRequest = make(map[uint64]bool)
	s.logsSequence = make(map[uint64]uint64)
	err = s.Start()
	return
}
