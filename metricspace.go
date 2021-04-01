package nnsearch

import (
	"container/heap"
	"io"
	"sort"
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

	for i := 0; i < bf.space.Length(); i++ {
		pt := bf.space.At(i)
		dist := bf.space.Distance(pt, target)
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
	}

	sort.Slice(results, func(a, b int) bool {
		return results[a].Distance < results[b].Distance
	})

	return results
}
