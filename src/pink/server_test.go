package pink

import (
	"math/rand"
	"testing"
	"time"
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
}
