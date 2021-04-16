package nnsearch

import (
	"io"
	"log"
	"math"
	"math/bits"
	"reflect"
)

type atter interface {
	At(i int) byte
}

type ByteInputStream interface {
	NextByte() byte
}

type byteInputStream struct {
	atter
	pos int
}

func newByteInputStream(atter atter, pos uint64) *byteInputStream {
	return &byteInputStream{atter, int(pos)}
}

func (bs *byteInputStream) NextByte() byte {
	ret := bs.atter.At(bs.pos)
	bs.pos++
	return ret
}

func (bs *byteInputStream) Read(p []byte) (n int, err error) {
	for i := 0; i < len(p); i++ {
		p[i] = bs.At(bs.pos)
		bs.pos++
	}
	return len(p), nil
}

func WriteThing(w io.Writer, thing interface{}) uint64 {
	switch v := thing.(type) {
	case uint64:
		return writeUint64(w, v)
	case string:
		return writeString(w, v)
	case float32:
		return writeFloat32(w, v)
	case float64:
		return writeFloat64(w, v)
	case []float32:
		return writeFloat32Slice(w, v)
	case *[]float32:
		return writeFloat32Slice(w, *v)
	case int64:
		return writeInt64(w, v)
	case int:
		return writeInt64(w, int64(v))
	}

	log.Panicf("Don't know how to write this type: %v", reflect.TypeOf(thing))
	return 0
}

func ReadThing(bs ByteInputStream, thing interface{}) uint64 {
	switch v := thing.(type) {
	case *uint64:
		return readUint64(bs, v)
	case *string:
		return readString(bs, v)
	case *float32:
		return readFloat32(bs, v)
	case *float64:
		return readFloat64(bs, v)
	case *[]float32:
		return readFloat32Slice(bs, v)
	case *int64:
		return readInt64(bs, v)
	case *int:
		var v2 int64
		l := readInt64(bs, &v2)
		*v = int(v2)
		return l
	default:
		log.Panicf("Don't know how to read this type: %v", reflect.TypeOf(thing))
		return 0
	}
}

func writeUint64(w io.Writer, n uint64) uint64 {
	//var err error
	if n < 0x7f {
		if w != nil {
			_, _ = w.Write([]byte{byte(n)})
		}
		return 1
	} else if n < 0x3fff {
		if w != nil {
			_, _ = w.Write([]byte{
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 2
	} else if n < 0x1fffff {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 3
	} else if n < 0xfffffff {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>21)&0x7f | 0x80),
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 4

	} else if n < 0x3ffffffff {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>28)&0x7f | 0x80),
				byte((n>>21)&0x7f | 0x80),
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 5
	} else if n < 0x3ffffffffff {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>35)&0x7f | 0x80),
				byte((n>>28)&0x7f | 0x80),
				byte((n>>21)&0x7f | 0x80),
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 6

	} else if n < 0x1ffffffffffff {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>42)&0x7f | 0x80),
				byte((n>>35)&0x7f | 0x80),
				byte((n>>28)&0x7f | 0x80),
				byte((n>>21)&0x7f | 0x80),
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 7
	} else if n < (1<<56)-1 {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>49)&0x7f | 0x80),
				byte((n>>42)&0x7f | 0x80),
				byte((n>>35)&0x7f | 0x80),
				byte((n>>28)&0x7f | 0x80),
				byte((n>>21)&0x7f | 0x80),
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 8
	} else if n < (1<<63)-1 {
		if w != nil {

			_, _ = w.Write([]byte{
				byte((n>>56)&0x7f | 0x80),
				byte((n>>49)&0x7f | 0x80),
				byte((n>>42)&0x7f | 0x80),
				byte((n>>35)&0x7f | 0x80),
				byte((n>>28)&0x7f | 0x80),
				byte((n>>21)&0x7f | 0x80),
				byte((n>>14)&0x7f | 0x80),
				byte((n>>7)&0x7f | 0x80),
				byte(n & 0x7f),
			})
		}
		return 9

	}
	if w != nil {
		_, _ = w.Write([]byte{
			byte((n>>63)&0x7f | 0x80),
			byte((n>>56)&0x7f | 0x80),
			byte((n>>49)&0x7f | 0x80),
			byte((n>>42)&0x7f | 0x80),
			byte((n>>35)&0x7f | 0x80),
			byte((n>>28)&0x7f | 0x80),
			byte((n>>21)&0x7f | 0x80),
			byte((n>>14)&0x7f | 0x80),
			byte((n>>7)&0x7f | 0x80),
			byte(n & 0x7f),
		})
	}
	return 10

}

func readUint64(bs ByteInputStream, v *uint64) uint64 {
	*v = 0
	var l uint64
	for {
		l++
		d := bs.NextByte()
		*v = (*v << 7) | uint64(d&0x7f)
		if d&0x80 == 0 {
			break
		}
	}
	return l
}

func writeString(w io.Writer, v string) uint64 {
	l := uint64(len(v))
	l += writeUint64(w, l)
	if w != nil {
		_, _ = w.Write([]byte(v))
	}
	return l
}

func readString(bs ByteInputStream, v *string) uint64 {
	var l uint64
	n := readUint64(bs, &l)
	b := make([]byte, l)
	for i := uint64(0); i < l; i++ {
		b[i] = bs.NextByte()
	}
	*v = string(b)
	return n + l
}

func writeFloat32(w io.Writer, v float32) uint64 {
	u := bits.ReverseBytes32(math.Float32bits(v))
	return writeUint64(w, uint64(u))
}

func readFloat32(bs ByteInputStream, v *float32) uint64 {
	var u uint64
	l := readUint64(bs, &u)
	*v = math.Float32frombits(bits.ReverseBytes32(uint32(u)))
	return l
}

func writeFloat64(w io.Writer, v float64) uint64 {
	u := bits.ReverseBytes64(math.Float64bits(v))
	return writeUint64(w, uint64(u))
}

func readFloat64(bs ByteInputStream, v *float64) uint64 {
	var u uint64
	l := readUint64(bs, &u)
	*v = math.Float64frombits(bits.ReverseBytes64(uint64(u)))
	return l
}

func writeFloat32Slice(w io.Writer, v []float32) uint64 {
	l := uint64(len(v))
	n := writeUint64(w, l)
	for i := uint64(0); i < l; i++ {
		n += writeFloat32(w, v[i])
	}
	return n
}

func readFloat32Slice(bs ByteInputStream, v *[]float32) uint64 {
	var l uint64
	n := readUint64(bs, &l)
	*v = make([]float32, l)
	for i := uint64(0); i < l; i++ {
		n += readFloat32(bs, &((*v)[i]))
	}
	return n
}

func writeInt64(w io.Writer, v int64) uint64 {
	var u uint64
	if v < 0 {
		u = (^uint64(v) << 1) | 1 // complement i, bit 0 is 1
	} else {
		u = (uint64(v) << 1) // do not complement i, bit 0 is 0
	}
	return writeUint64(w, u)
}

func readInt64(bs ByteInputStream, v *int64) uint64 {
	var u uint64
	n := readUint64(bs, &u)
	if (u & 1) == 1 {
		*v = int64(^(u >> 1))
	} else {
		*v = int64(u >> 1)
	}
	return n
}
