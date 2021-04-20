package nnsearch

import (
	"math"
	"math/rand"
	"runtime"
	"sync"
)

func Mean(arr []float64) float64 {
	if len(arr) < 1 {
		return 0
	}

	mean := float64(0)
	for _, value := range arr {
		mean += value
	}

	return mean / float64(len(arr))
}

func Variance(arr []float64) float64 {
	if len(arr) <= 1 {
		return 0
	}

	m := Mean(arr)
	s2 := float64(0)
	for _, v := range arr {
		s2 += (v - m) * (v - m)
	}
	return s2 / float64((len(arr) - 1))
}

func ArgmaxFn(n int, fn func(i int) float64) int {
	if n <= 0 {
		return -1
	}

	bestI := 0
	bestV := fn(0)
	for i := 0; i < n; i++ {
		v := fn(i)
		if v > bestV {
			bestV = v
			bestI = i
		}
	}
	return bestI
}

func Map(n int, fn func(i int) float64) []float64 {
	ret := make([]float64, n)
	for i := 0; i < n; i++ {
		ret[i] = fn(i)
	}
	return ret
}

func Sequence(n int) []int {
	ret := make([]int, n)
	for i := 0; i < n; i++ {
		ret[i] = i
	}
	return ret
}

func EuclideanDistance(vec1 []float32, vec2 []float32) float64 {
	var sum float64
	for i := range vec1 {
		sum += float64((vec1[i] - vec2[i]) * (vec1[i] - vec2[i]))
	}
	return math.Sqrt(sum)
}

func CosineDistance(vec1 []float32, vec2 []float32) float64 {

	var len1, len2, dot float64
	for i := range vec1 {
		len1 += float64(vec1[i] * vec1[i])
		len2 += float64(vec2[i] * vec2[i])
		dot += float64(vec1[i] * vec2[i])
	}

	numer := dot / (math.Sqrt(len1) * math.Sqrt(len2))
	if numer > 1.0 {
		numer = 1.0
	} else if numer < -1.0 {
		numer = -1.0
	}
	return math.Acos(numer) / math.Pi
}

func ForkLoop(n int, fn func(i int)) {
	threads := runtime.NumCPU()
	var wg sync.WaitGroup

	worker := func(offset int) {
		for i := offset; i < n; i += threads {
			fn(i)
		}
		wg.Done()
	}

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go worker(i)
	}

	wg.Wait()
}

func BatchedForkLoop(n, batchSize int, fn func(start, end int)) {
	threads := runtime.NumCPU()
	var wg sync.WaitGroup

	worker := func(offset int) {
		for i := offset; i < n/batchSize; i += threads {
			start := i * batchSize
			end := start + batchSize
			if end > n {
				end = n
			}
			fn(start, end)
		}
		wg.Done()
	}

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go worker(i)
	}

	wg.Wait()
}

func ForkWhile(fn func() bool) {
	threads := runtime.NumCPU()
	var wg sync.WaitGroup

	worker := func() {
		for fn() {
		}
		wg.Done()
	}

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go worker()
	}

	wg.Wait()
}

func ShuffledForkLoop(n int, fn func(i int)) {
	order := Sequence(n)
	rand.Shuffle(n, func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
	ForkLoop(n, func(i int) {
		fn(order[i])
	})
}
