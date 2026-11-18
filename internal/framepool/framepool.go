package framepool

import "sync"

const MaxPoolSize = 4 << 20

// Each class's sync.Pool stores a pointer to a fixed-size array, which fits in
// an interface value without heap-allocating a slice header on Get or Put.
type (
	buf4k   [4 << 10]byte
	buf16k  [16 << 10]byte
	buf64k  [64 << 10]byte
	buf256k [256 << 10]byte
	buf1m   [1 << 20]byte
	buf4m   [4 << 20]byte
)

var pools = [...]sync.Pool{
	{New: func() any { return &buf4k{} }},
	{New: func() any { return &buf16k{} }},
	{New: func() any { return &buf64k{} }},
	{New: func() any { return &buf256k{} }},
	{New: func() any { return &buf1m{} }},
	{New: func() any { return &buf4m{} }},
}

// Get returns an empty slice with capacity for at least minLength bytes,
// suitable for appending into.
func Get(minLength int) []byte {
	switch {
	case minLength <= 4<<10:
		return pools[0].Get().(*buf4k)[:0]
	case minLength <= 16<<10:
		return pools[1].Get().(*buf16k)[:0]
	case minLength <= 64<<10:
		return pools[2].Get().(*buf64k)[:0]
	case minLength <= 256<<10:
		return pools[3].Get().(*buf256k)[:0]
	case minLength <= 1<<20:
		return pools[4].Get().(*buf1m)[:0]
	case minLength <= 4<<20:
		return pools[5].Get().(*buf4m)[:0]
	default:
		return make([]byte, 0, minLength)
	}
}

func Put(data []byte) {
	switch cap(data) {
	case 4 << 10:
		pools[0].Put((*buf4k)(data[:4<<10]))
	case 16 << 10:
		pools[1].Put((*buf16k)(data[:16<<10]))
	case 64 << 10:
		pools[2].Put((*buf64k)(data[:64<<10]))
	case 256 << 10:
		pools[3].Put((*buf256k)(data[:256<<10]))
	case 1 << 20:
		pools[4].Put((*buf1m)(data[:1<<20]))
	case 4 << 20:
		pools[5].Put((*buf4m)(data[:4<<20]))
	}
}
