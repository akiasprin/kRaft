package pink

import (
	"math/rand"
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

func recvResultChan(i int, server *Server, seq uint64,
	content []byte, resultChan chan bool) {
	select {
	case result := <-resultChan:
		if result {
			ewlog.Infof("[%v seq-%v]:提交成功:Log:%v:MatchIndex:%v\n", i, seq, server.Logs, server.MatchIndex)
		} else {
			ewlog.Errorf("[%v seq-%v]:提交失败:Log:%v:MatchIndex:%v\n", i, seq, server.Logs, server.MatchIndex)
			time.Sleep(100 * time.Millisecond)
			go submit(i, server, seq, content)
		}
	}
}

func submit(i int, server *Server, seq uint64, content []byte) {
	resultChan := server.Push(seq, content)
	go recvResultChan(i, server, seq, content, resultChan)
}
func randomTimeout() time.Duration {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := rand.Intn(rTimeout)
	return time.Millisecond * time.Duration(rTimeout+r)
}

func TestCase(t *testing.T) {
	ewlog.SetLogLevel(2)

	var err error
	var servers [NUM]*Server
	var applyChans [NUM]chan *ApplyMsg

	perfixAddr := "127.0.0.1"
	startPort := 6033
	serverList := []string{}

	var seq uint64
	seq = 0

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

	time.Sleep(time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		go func(i int) {
			if isLeader {
				seq++
				content := []byte("AAA")
				ewlog.Infof("[%v]:日志添加:%+v", i, content)
				submit(i, servers[i], seq, content)
				time.Sleep(time.Second)
				servers[i].Close()
				if true {
					servers[(i+1)%NUM].Close()
					servers[(i+2)%NUM].Close()
					time.Sleep(time.Second)
				}
			}
		}(i)
	}

	time.Sleep(time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		go func(i int) {
			if isLeader {
				seq++
				content := []byte("CCC")
				ewlog.Infof("[%v]:日志添加:%+v", i, content)
				submit(i, servers[i], seq, content)
			}
		}(i)
	}

	time.Sleep(time.Second)

	xor := true
	var mu sync.Mutex
	for i := 0; i < NUM; i++ {
		_, isLeader, state := servers[i].GetState()
		go func(i int) {
			if isLeader {
				seq++
				content := []byte("BBB")
				ewlog.Infof("[%v]:日志添加:%+v", i, content)
				submit(i, servers[i], seq, content)
			}
			mu.Lock()
			if state == CLOSED && xor == true {
				servers[i].Start()
				ewlog.Infof("Starting %v...\n", i)
				// xor = false
			}
			mu.Unlock()
		}(i)
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < NUM; i++ {
		_, isLeader, _ := servers[i].GetState()
		go func(i int) {
			if isLeader {
				seq++
				content := []byte("DDD")
				ewlog.Infof("[%v]:日志添加:%+v", i, content)
				submit(i, servers[i], seq, content)
			}
		}(i)
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < NUM; i++ {
		ewlog.Info(servers[i].GetState())
		ewlog.Infof("[%v]:%v:%v\n", i, servers[i].Logs, servers[i].MatchIndex)
	}
}
