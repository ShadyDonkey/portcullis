package framepool

import (
	"sync"
	"sync/atomic"
)

const MaxPoolSize = 4 << 20

var poolSizes = [...]int{
	4 << 10,
	16 << 10,
	64 << 10,
	256 << 10,
	1 << 20,
	4 << 20,
}

var pools [len(poolSizes)]sync.Pool
var oversizedFrames atomic.Uint64

// Get returns an empty slice with capacity for at least minLength bytes,
// suitable for appending into.
func Get(minLength int) []byte {
	for i, size := range poolSizes {
		if minLength <= size {
			if v := pools[i].Get(); v != nil {
				return v.([]byte)
			}

			return make([]byte, 0, size)
		}
	}

	oversizedFrames.Add(1)
	return make([]byte, 0, minLength)
}

func Put(data []byte) {
	for i, size := range poolSizes {
		if cap(data) == size {
			pools[i].Put(data[:0])
			return
		}
	}
}

func OversizedFrames() uint64 {
	return oversizedFrames.Load()
}
