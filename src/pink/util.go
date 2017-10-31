package pink

import (
	"math/rand"
	"time"
)

const (
	MinElectTimeout   = 300
	ElectTimeoutRandN = 500
	HeartbeatTimeout  = 60 * time.Millisecond
)

func getMin(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func electionTimeout() time.Duration {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := rand.Intn(ElectTimeoutRandN)
	return time.Millisecond * time.Duration(MinElectTimeout+r)
}
