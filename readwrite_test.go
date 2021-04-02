package nnsearch

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"testing"
)

type testStruct struct {
	hello  string
	value1 float32
	value2 []float32
	value3 int64
}

func (ts *testStruct) Encode(w io.Writer) uint64 {
	var l uint64
	l += WriteThing(w, ts.hello)
	l += WriteThing(w, ts.value1)
	l += WriteThing(w, ts.value2)
	l += WriteThing(w, ts.value3)
	return l
}

func (ts *testStruct) Decode(bs ByteInputStream) {
	ReadThing(bs, &ts.hello)
	ReadThing(bs, &ts.value1)
	ReadThing(bs, &ts.value2)
	ReadThing(bs, &ts.value3)
}

func (ts *testStruct) String() string {
	return fmt.Sprintf("%v", *ts)
}

func TestReadWrite(t *testing.T) {
	t.Logf("Run read/write test")

	var written1 testStruct
	written1.hello = "hello"
	written1.value1 = 1.5
	written1.value2 = []float32{1.0, 2.0, 3.0}
	written1.value3 = -3

	items := []FrozenItem{&written1, &written1}

	f, err := os.Create("freezetest.dat")
	if err != nil {
		panic(err)
	}

	b := bufio.NewWriter(f)

	FreezeItems(b, items)
	b.Flush()
	f.Close()

	ff := OpenFrozenFile("freezetest.dat")

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
}
