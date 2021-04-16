package nnsearch

import (
	"container/heap"
	"context"
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

type PointFilter = func(pt Point) bool

func AllowAll(pt Point) bool {
	return true
}

// Options for nearest neighbour searching. All options are optional.
type SearchOptions struct {
	// A context that can abort the search.
	Ctx context.Context

	// A method that returns true if a point is admissible.
	Filter PointFilter
}

func getOptions(in *SearchOptions) *SearchOptions {
	var out SearchOptions
	if in != nil {
		out = *in
	}

	if out.Ctx == nil {
		out.Ctx = context.Background()
	}

	if out.Filter == nil {
		out.Filter = AllowAll
	}

	return &out
}

type SpaceIndex interface {
	NearestNeighbours(target Point, k int, options *SearchOptions) []PointDistance
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

func (bf *bruteForceIndex) NearestNeighbours(target Point, k int, options *SearchOptions) []PointDistance {
	opt := getOptions(options)
	results := make(pointHeap, 0, k)
	var mutex sync.Mutex

	ForkLoop(bf.space.Length(), func(i int) {
		if opt.Ctx.Err() != nil {
			return
		}

		pt := bf.space.At(i)
		if !opt.Filter(pt) {
			return
		}

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

func ComputeDistances(space MetricSpace, pt Point) []float64 {
	ret := make([]float64, space.Length())
	for i := 0; i < space.Length(); i++ {
		ret[i] = space.Distance(pt, space.At(i))
	}
	return ret
}

type subspace struct {
	MetricSpace
	k int
}

// NewSubspace
func NewSubspace(space MetricSpace, k int) MetricSpace {
	if k > space.Length() {
		k = space.Length()
	}
	return subspace{space, k}
}

func (ss subspace) Length() int {
	return ss.k
}
