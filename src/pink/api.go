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
	if s.clientRequest[seq] != 0 {
		success = s.Logs[s.clientRequest[seq]].Seq == seq
		go func(success bool) {
			resultChan <- success
		}(success)
		return
	}
	nextIndex := s.getLastIndex() + 1
	s.clientRequest[seq] = nextIndex
	s.Logs = append(s.Logs, protodef.LogEntry{
		Seq:     seq,
		Term:    s.CurrentTerm,
		Index:   nextIndex,
		Command: cmd,
	})
	go func(success bool) {
		for i := 0; i < MAXRETRY; i++ {
			s.mu.Lock()
			if s.CommitIndex >= nextIndex {
				if s.Logs[nextIndex].Seq == seq {
					success = true
				} else {
					s.mu.Unlock()
					break
				}
			}
			s.mu.Unlock()
			time.Sleep(SLEEPINTERVAL)
		}
		resultChan <- success
	}(success)
	return
}
