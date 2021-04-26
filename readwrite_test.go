package nnsearch

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
)

type testStruct struct {
	Hello  string
	Value1 float32
	Value2 []float32
	Value3 int64
}

func (ts *testStruct) Encode(w io.Writer) uint64 {
	var l uint64
	l += WriteThing(w, ts.Hello)
	l += WriteThing(w, ts.Value1)
	l += WriteThing(w, ts.Value2)
	l += WriteThing(w, ts.Value3)
	return l
}

func (ts *testStruct) Decode(bs ByteInputStream) {
	ReadThing(bs, &ts.Hello)
	ReadThing(bs, &ts.Value1)
	ReadThing(bs, &ts.Value2)
	ReadThing(bs, &ts.Value3)
}

func (ts *testStruct) String() string {
	return fmt.Sprintf("%v", *ts)
}

func TestReadWrite(t *testing.T) {
	t.Logf("Run read/write test")

	var written1 testStruct
	written1.Hello = "hello"
	written1.Value1 = 1.5
	written1.Value2 = []float32{1.0, 2.0, 3.0}
	written1.Value3 = -3

	items := []FrozenItem{&written1, &written1}

	f, err := os.Create("freezetest.dat")
	if err != nil {
		panic(err)
	}

	b := bufio.NewWriter(f)

	FreezeItems(b, items)
	b.Flush()
	f.Close()

	ff, _ := OpenFrozenFile("freezetest.dat")

	if ff.GetCount() != 2 {
		log.Panicf("getCount returned %v", ff.GetCount())
	}

	var read testStruct
	ff.GetItem(0, &read)
	str := (&read).String()
	if str != "{hello 1.5 [1 2 3] -3}" {
		log.Panicf("Read incorrect value, got %v", str)
	}

	ff.GetItem(1, &read)
	str = (&read).String()
	if str != "{hello 1.5 [1 2 3] -3}" {
		log.Panicf("Read incorrect value, got %v", str)
	}

	slice := []*testStruct{&written1, &written1}
	var buff bytes.Buffer
	Encode(&buff, slice)
	log.Printf("Encoded to %v", buff.Bytes())
}
