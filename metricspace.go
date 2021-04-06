package nnsearch

import (
	"container/heap"
	"io"
	"math/rand"
	"sort"
	"sync"
)

type Point interface {
}

type MetricSpace interface {
	Length() int
	At(i int) Point
	Distance(p1, p2 Point) float64
}

type PointDistance struct {
	Index    int
	Point    Point
	Distance float64
}

type SpaceIndex interface {
	NearestNeighbours(target Point, k int) []PointDistance
	Write(w io.Writer) (int64, error)
}

type pointHeap []PointDistance

func (h pointHeap) Len() int           { return len(h) }
func (h pointHeap) Less(i, j int) bool { return h[i].Distance > h[j].Distance }
func (h pointHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *pointHeap) Push(x interface{}) {
	*h = append(*h, x.(PointDistance))
}

func (h *pointHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

/** Brute force index */
type bruteForceIndex struct {
	space MetricSpace
}

func NewBruteForceIndex(space MetricSpace) SpaceIndex {
	return &bruteForceIndex{space}
}

func (bf *bruteForceIndex) Write(w io.Writer) (int64, error) {
	return 0, nil
}

func (bf *bruteForceIndex) NearestNeighbours(target Point, k int) []PointDistance {
	results := make(pointHeap, 0, k)
	counter := NewCounter(100)
	var mutex sync.Mutex

	ForkLoop(bf.space.Length(), func(i int) {
		pt := bf.space.At(i)
		dist := bf.space.Distance(target, pt)
		mutex.Lock()
		if len(results) < k || results[0].Distance > dist {
			if len(results) == k {
				heap.Pop(&results)
			}
			heap.Push(&results, PointDistance{
				Index:    i,
				Point:    pt,
				Distance: dist,
			})
		}
		counter.Count()
		mutex.Unlock()
	})

	sort.Slice(results, func(a, b int) bool {
		return results[a].Distance < results[b].Distance
	})

	return results
}

func ComputeAverageDistance(space MetricSpace, samples int) float64 {
	// find cutoff
	sum := float64(0)
	n := 0
	for i := 0; i < 1000; i++ {
		n1 := rand.Intn(space.Length())
		n2 := rand.Intn(space.Length())
		if n1 != n2 {
			n++
			sum += space.Distance(space.At(n1), space.At(n2))
		}
	}

	return sum / float64(n)
}
