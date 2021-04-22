package nnsearch

import (
	"encoding/binary"
	"io"
	"log"

	"golang.org/x/exp/mmap"
)

type FrozenFile struct {
	r      atter
	offset uint64
	count  int64
}

type FrozenItem interface {
	Decode(r ByteInputStream)
	Encode(w io.Writer) uint64
}

func FreezeItems(w io.Writer, items []FrozenItem) uint64 {
	// write number of items
	off := WriteThing(w, uint64(len(items))) + 4*uint64(len(items))
	// write offset of items
	for _, item := range items {
		err := binary.Write(w, binary.BigEndian, uint32(off))
		if err != nil {
			log.Panic(err)
		}
		off += item.Encode(nil)
	}
	// write items
	for _, item := range items {
		item.Encode(w)
	}

	return off
}

func (ff *FrozenFile) GetCount() int64 {
	return ff.count
}

func (ff *FrozenFile) GetItem(index int, item FrozenItem) {
	offset1 := int(ff.offset + uint64(index*4))
	offset2 := uint64(ff.r.At(offset1))<<24 |
		uint64(ff.r.At(offset1+1))<<16 |
		uint64(ff.r.At(offset1+2))<<8 |
		uint64(ff.r.At(offset1+3))
	//log.Printf("Item %v at offset %v", index, offset2)
	bs := newByteInputStream(ff.r, offset2)
	item.Decode(bs)
}

func (ff *FrozenFile) Close() error {
	if c, ok := ff.r.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func OpenFrozenFile(filename string) (*FrozenFile, error) {
	file, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}

	bs := newByteInputStream(file, 0)
	var count uint64
	off := ReadThing(bs, &count)

	return &FrozenFile{
		offset: off,
		r:      file,
		count:  int64(count),
	}, nil
}
