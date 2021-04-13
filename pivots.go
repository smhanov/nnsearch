package nnsearch

import (
	"math"
	"math/rand"
	"sort"
)

type Pivot struct {
	Index     int
	Distances []float64
	Variance  float64
}

func ChoosePivots(space MetricSpace, k int) []Pivot {
	var pivots []Pivot
	if k > space.Length() {
		k = space.Length()
	}

	if k == 0 {
		return pivots
	}

	have := make(map[int]bool)

	// choose a random point to start from. It will be discarded later.
	pt := rand.Intn(space.Length())

	usePivot := func(pt int) {
		pivot := Pivot{
			Index:     pt,
			Distances: ComputeDistances(space, space.At(pt)),
		}
		pivot.Variance = Variance(pivot.Distances)
		pivots = append(pivots, pivot)
		have[pt] = true
	}

	usePivot(pt)

	// find two more points farthest from it
	for j := 0; j < 2; j++ {
		pt = ArgmaxFn(space.Length(), func(i int) float64 {
			if math.IsInf(pivots[j].Distances[i], 1) {
				return -1
			}
			return pivots[j].Distances[i]
		})

		usePivot(pt)
	}

	// discard the first point
	pivots = pivots[1:]

	for len(pivots) < k {
		// for other points, find point with the least variance in distance from the pivots that are not already chosen.
		variances := Map(space.Length(), func(i int) float64 {
			return Variance(Map(len(pivots), func(j int) float64 {
				return pivots[j].Distances[i]
			}),
			)
		})

		pt = ArgmaxFn(len(variances), func(i int) float64 {
			if have[i] {
				return math.Inf(-1)
			}
			return -variances[i]
		})

		usePivot(pt)
	}

	return pivots
}

func PivotHash(pivots []Pivot, u int) []int {
	result := Sequence(len(pivots))
	sort.Slice(Sequence(len(pivots)), func(a, b int) bool {
		return pivots[a].Distances[u] < pivots[b].Distances[u]
	})
	return result
}

func PivotHashLessThan(a, b []int) bool {
	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return true
		}

		if a[i] > b[i] {
			break
		}
	}

	return false
}
