package pink

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ender-wan/ewlog"
)

const NUM = 5

const (
	rTimeout = 600
)

var servers [NUM]*Server
var serverList []string

func recvResultChan(index, term *uint64, id int, server *Server,
	content []byte, resultChan chan int, count int) {
	select {
	case result := <-resultChan:
		if result == COMMITED {
			ewlog.Infof("[%v]:[T/%v|I/%v]提交成功:Log:%v:MatchIndex:%v\n", id, *term, *index, server.Logs, server.MatchIndex)
		} else if result == UNCOMMITED {
			ewlog.Infof("[%v]:[T/%v|I/%v]提交中:Log:%v:MatchIndex:%v\n", id, *term, *index, server.Logs, server.MatchIndex)
			time.Sleep(100 * time.Millisecond)
			t_id, t_server := findLeader()
			if t_id != -1 {
				id = t_id
				server = t_server
			}
			go submit(index, term, id, server, content, count+1)
		} else {
			ewlog.Infof("[%v]:[T/%v|I/%v]过期提交:Log:%v:MatchIndex:%v\n", id, *term, *index, server.Logs, server.MatchIndex)
		}
	}
}

func submit(index, term *uint64, id int, server *Server, content []byte, count int) {
	if count >= 30 {
		//提交失败需要原使用index, term提交直至提交出结果，否则均不能保证提交结果
		ewlog.Infof("[%v]:[T/%v|I/%v]提交失败:Log:%v\n", id, *term, *index, content)
		return
	}
	resultChan := server.Push(index, term, content)
	ewlog.Infof("[%v]:[T/%v|I/%v]提交:Log:%v\n", id, *term, *index, content)
	go recvResultChan(index, term, id, server, content, resultChan, count)
}

func findLeader() (int, *Server) {
	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		if isLeader {
			return i, servers[i]
		}
	}
	return -1, nil
}
func TestCase(t *testing.T) {
	// ewlog.SetLogLevel(1)

	var err error
	var applyChans [NUM]chan *ApplyMsg

	perfixAddr := "127.0.0.1"
	startPort := 6033

	for i := 0; i < NUM; i++ {
		serverList = append(serverList, perfixAddr+":"+strconv.Itoa(startPort+i))
		applyChans[i] = make(chan *ApplyMsg, 5000)
	}

	for i := 0; i < NUM; i++ {
		servers[i], err = NewRaftServer(
			serverList,
			i,
			startPort+i,
			applyChans[i])
		if err != nil {
			ewlog.Error(err)
			return
		}
	}

	time.Sleep(3 * time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		go func(i int) {
			if isLeader {
				var index, term uint64
				content := []byte("A")
				submit(&index, &term, i, servers[i], content, 0)
				servers[i].Close()
				time.Sleep(time.Second)
				if true {
					servers[(i+1)%NUM].Close()
					time.Sleep(time.Second)
				}
			}
		}(i)
	}

	time.Sleep(3 * time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		go func(i int) {
			if isLeader {
				var index, term uint64
				content := []byte("B")
				servers[(i+1)%NUM].Close()
				servers[(i+2)%NUM].Close()
				time.Sleep(time.Second)
				submit(&index, &term, i, servers[i], content, 0)
				servers[i].Close()
				time.Sleep(time.Second)
			}
		}(i)
	}

	time.Sleep(3 * time.Second)

	xor := true
	var mu sync.Mutex
	for i := 0; i < NUM; i++ {
		_, isLeader, state := servers[i].GetState()
		go func(i int) {
			if isLeader {
				var index, term uint64
				content := []byte("C")
				submit(&index, &term, i, servers[i], content, 0)
				time.Sleep(time.Second)
			}
			mu.Lock()
			if state == CLOSED && xor == true {
				servers[i].Start()
				ewlog.Infof("[%v]:启动中..\n", i)
				xor = false
			}
			mu.Unlock()
		}(i)
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, state := servers[i].GetState()
		go func(i int) {
			if isLeader {
				var index, term uint64
				content := []byte("D")
				submit(&index, &term, i, servers[i], content, 0)
			}
			if state == CLOSED {
				servers[i].Start()
				ewlog.Infof("[%v]:启动中..\n", i)
			}
		}(i)
	}

	time.Sleep(time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		go func(i int) {
			if isLeader {
				var index, term uint64
				content := []byte("E")
				submit(&index, &term, i, servers[i], content, 0)
			}
		}(i)
	}

	time.Sleep(15 * time.Second)

	for i := 0; i < NUM; i++ {
		term, leader, state := servers[i].GetState()
		if leader {

			ewlog.Infof("[%v]:状态:Term:%v:Code:%v:！领袖！CommitIndex:%v", i, term, state, servers[i].CommitIndex)
		} else {
			ewlog.Infof("[%v]:状态:Term:%v:Code:%v", i, term, state)
		}
		ewlog.Infof("[%v]:日志:%v\n", i, servers[i].Logs)
	}
}
