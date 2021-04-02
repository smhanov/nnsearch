package nnsearch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	_ "net/http/pprof"
	"strings"
	"sync"

	"golang.org/x/exp/mmap"
)

type WordVector struct {
	Vector []float32
	Word   string
}

func (wv *WordVector) String() string {
	return wv.Word
}

// WordVecs ...
type WordVecs struct {
	d     int
	n     int
	file  *mmap.ReaderAt
	index map[string]int64
	cache map[string][]float32
	all   []string
	mutex sync.Mutex
}

func readUntil(file *mmap.ReaderAt, at int, ch byte) (string, int) {
	var result strings.Builder
	for at < file.Len() {
		b := file.At(at)
		at++
		if b == ch {
			break
		}
		result.WriteByte(b)
	}
	return result.String(), at
}

// OpenWordVecs ...
func OpenWordVecs(filename string) *WordVecs {
	file, err := mmap.Open(filename)
	if err != nil {
		log.Panic(err)
	}

	wv := &WordVecs{
		file:  file,
		index: make(map[string]int64),
		cache: make(map[string][]float32),
	}

	header, at := readUntil(file, 0, 0x0a)
	fmt.Sscanf(header, "%d %d", &wv.n, &wv.d)
	log.Printf("Read %v words", wv.n)

	var word string
	for i := 0; i < wv.n; i++ {
		word, at = readUntil(file, at, 0x20)
		wv.index[word] = int64(at)
		wv.all = append(wv.all, word)
		at += wv.d*4 + 1
	}

	//for word := range wv.index {
	//	wv.cache[word] = wv.Get(word)
	//}
	return wv
}

// Get ...
func (wv *WordVecs) Get(word string) []float32 {
	wv.mutex.Lock()
	defer wv.mutex.Unlock()
	if item := wv.cache[word]; item != nil {
		return item
	}
	pos, ok := wv.index[word]
	if !ok {
		return nil
	}

	result := make([]float32, wv.d)
	b := make([]byte, 4*wv.d)
	_, err := wv.file.ReadAt(b, pos)
	if err != nil {
		log.Panic(err)
	}

	buf := bytes.NewReader(b)
	err = binary.Read(buf, binary.LittleEndian, result)
	if err != nil {
		log.Panic(err)
	}
	wv.cache[word] = result
	return result
}

func CosineDistance(vec1 []float32, vec2 []float32) float64 {
	var sum float64
	for i := range vec1 {
		sum += float64((vec1[i] - vec2[i]) * (vec1[i] - vec2[i]))
	}
	return math.Sqrt(sum)
}

// Distance ...
func (wv *WordVecs) Distance(word1 string, word2 string) float64 {
	vec1 := wv.Get(word1)
	vec2 := wv.Get(word2)
	return CosineDistance(vec1, vec2)
}

func (wv *WordVecs) Length() int {
	return len(wv.all)
}

func (wv *WordVecs) At(i int) Point {
	return &WordVector{
		Word:   wv.all[i],
		Vector: wv.Get(wv.all[i]),
	}
}

func (wv *WordVecs) PointFromQuery(text string) *WordVector {
	total := make([]float32, wv.d)
	for _, word := range tokenize(text) {
		vec := wv.Get(word)
		for i := range vec {
			total[i] += vec[i]
		}
	}

	return &WordVector{
		Word:   text,
		Vector: total,
	}
}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	return strings.Split(text, " ")
}
