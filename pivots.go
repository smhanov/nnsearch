package nnsearch

import (
	"math"
	"math/rand"
	"sort"
	"sync"
)

type Pivot struct {
	Index     int
	Distances []float64
	Variance  float64
}

type Pivots []Pivot

func ChoosePivots(space MetricSpace) Pivots {
	k := int(math.Max(math.Log2(float64(space.Length())), 3))
	return ChooseKPivots(space, k)
}

func ChooseKPivots(space MetricSpace, k int) Pivots {
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

func (pivots Pivots) Hash(u int) []int {
	result := Sequence(len(pivots))
	sort.Slice(Sequence(len(pivots)), func(a, b int) bool {
		return pivots[a].Distances[u] < pivots[b].Distances[u]
	})
	return result
}

// Returns the minumum and maximum distances that u and v can be
// from eachother. The true distance of u/v lie in this range.
func (pivots Pivots) ApproxDistance(u, v int) (min, max float64) {
	min = 0
	max = math.Inf(1)
	for _, pivot := range pivots {
		closest := math.Abs(pivot.Distances[u] - pivot.Distances[v])
		farthest := pivot.Distances[u] + pivot.Distances[v]

		if farthest < max {
			max = farthest
		}

		if closest > min {
			min = closest
		}

	}
	return min, max
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

func (pivots Pivots) RangeQueryByIndex(space MetricSpace, u int, radius float64, options *SearchOptions) []PointDistance {
	var results []PointDistance

	if len(pivots) == 0 {
		return results
	}

	options = getOptions(options)

	n := len(pivots[0].Distances)
	var mutex sync.Mutex
	upt := space.At(u)
	ForkLoop(n, func(v int) {
		if v != u {
			min, _ := pivots.ApproxDistance(u, v)
			if min <= radius {
				vpt := space.At(v)
				if !options.Filter(vpt) {
					return
				}
				dist := space.Distance(upt, vpt)
				if dist <= radius {
					mutex.Lock()
					defer mutex.Unlock()
					results = append(results, PointDistance{
						Index:    v,
						Point:    vpt,
						Distance: dist,
					})
				}
			}
		}
	})

	sort.Slice(results, func(a, b int) bool {
		return results[a].Distance < results[b].Distance
	})

	return results
}
