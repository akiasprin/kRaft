package pink

import (
	"pink/protodef"
	"time"
)

const (
	MAXRETRY      = 10
	SLEEPINTERVAL = 300 * time.Millisecond
)

func (s *Server) Push(seq uint64, cmd []byte) (resultChan chan bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	success := false
	resultChan = make(chan bool)
	if s.clientRequest[seq] {
		resultChan <- success
		return
	}
	s.clientRequest[seq] = true
	nextIndex := s.getLastIndex() + 1
	s.Logs = append(s.Logs, protodef.LogEntry{
		Term:    s.CurrentTerm,
		Index:   nextIndex,
		Command: cmd,
	})
	s.logsSequence[nextIndex] = seq
	go func() {
		for i := 0; i < MAXRETRY && !success; i++ {
			s.mu.Lock()
			if s.CommitIndex >= nextIndex &&
				s.logsSequence[nextIndex] == seq {
				success = true
			}
			s.mu.Unlock()
			time.Sleep(SLEEPINTERVAL)
		}
		resultChan <- success
	}()
	return
}
