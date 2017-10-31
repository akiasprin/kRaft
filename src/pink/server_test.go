package pink

import (
	"math/rand"
	"pink/protodef"
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

func randomTimeout() time.Duration {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := rand.Intn(rTimeout)
	return time.Millisecond * time.Duration(rTimeout+r)
}

func TestCase(t *testing.T) {
	ewlog.SetLogLevel(0)
	var err error
	var servers [NUM]*Server
	var applyChans [NUM]chan *ApplyMsg

	perfixAddr := "127.0.0.1"
	startPort := 5550
	serverList := []string{}

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
	var wg sync.WaitGroup

	for i := 0; i < NUM; i++ {
		term, isLeader, _ := servers[i].GetState()
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			if isLeader {
				servers[i].Logs = append(servers[i].Logs, protodef.LogEntry{
					Term:    term,
					Index:   uint64(len(servers[i].Logs)),
					Command: []byte("AAA"),
				})
				ewlog.Infof("往Leader %v添加日志:%v", i, servers[i].Logs)
				time.Sleep(randomTimeout())
				ewlog.Infof("关闭 %v", i)
				servers[i].Close()
				if true {
					servers[(i+1)%NUM].Close()
					ewlog.Infof("关闭 %v", (i+1)%NUM)
					time.Sleep(randomTimeout())
				}
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(randomTimeout())

	for i := 0; i < NUM; i++ {
		term, isLeader, _ := servers[i].GetState()
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			if isLeader {
				servers[i].Logs = append(servers[i].Logs, protodef.LogEntry{
					Term:    term,
					Index:   uint64(len(servers[i].Logs)),
					Command: []byte("CCC"),
				})
				ewlog.Infof("往Leader %v添加日志:%v", i, servers[i].Logs)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(randomTimeout())

	xor := true
	var mu sync.Mutex
	for i := 0; i < NUM; i++ {
		term, isLeader, state := servers[i].GetState()
		go func(i int) {
			wg.Add(1)
			defer wg.Done()
			if isLeader {
				servers[i].Logs = append(servers[i].Logs, protodef.LogEntry{
					Term:    term,
					Index:   uint64(len(servers[i].Logs)),
					Command: []byte("BBB"),
				})
				ewlog.Infof("往Leader %v添加日志:%v", i, servers[i].Logs)
			}
			mu.Lock()
			if state == CLOSED && xor == true {
				servers[i].Start()
				ewlog.Infof("启动 %v...\n", i)
				xor = false
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	time.Sleep(time.Second)

	for i := 0; i < NUM; i++ {
		ewlog.Info(servers[i].GetState())
		ewlog.Info(servers[i].Logs)
	}
}
