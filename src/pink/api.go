package pink

import (
	"pink/protodef"
	"time"
)

const (
	MAXRETRY      = 10
	SLEEPINTERVAL = 300 * time.Millisecond

	COMMITED   = 1
	UNCOMMITED = 2
	EXPIRED    = 3
)

func (s *Server) isCommited(index, term uint64, resultChan chan int) {
	success := UNCOMMITED
	for i := 0; i < MAXRETRY; i++ {
		s.mu.Lock()
		if s.CommitIndex >= index {
			if s.Logs[index].Term == term {
				success = COMMITED
			} else {
				success = EXPIRED
				s.mu.Unlock()
				break
			}
		}
		s.mu.Unlock()
		time.Sleep(SLEEPINTERVAL)
	}
	resultChan <- success
}

func (s *Server) Push(index, term *uint64, cmd []byte) (resultChan chan int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resultChan = make(chan int)

	//如果不存在
	if *index == 0 {
		*index = s.getLastIndex() + 1
		*term = s.CurrentTerm
		s.Logs = append(s.Logs, protodef.LogEntry{
			Term:    s.CurrentTerm,
			Index:   *index,
			Command: cmd,
		})
	} else {
		//不是这条
		if s.Logs[*index].Term != *term {
			go func() { resultChan <- EXPIRED }()
			return
		} else {
			go func() { resultChan <- COMMITED }()
			return
		}
	}
	go s.isCommited(*index, *term, resultChan)
	return
}
