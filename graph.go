package nnsearch

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/bits"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
)

type graph struct {
	space   MetricSpace
	heaps   []edgeHeap
	checked map[pair]bool
	lock    sync.RWMutex
	locks   []sync.Mutex
}

type edge struct {
	index    int
	distance float64
	mark     bool
}

type pair struct {
	a, b int
}

type edgeHeap []edge

func (h edgeHeap) Len() int           { return len(h) }
func (h edgeHeap) Less(i, j int) bool { return h[i].distance > h[j].distance }
func (h edgeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *edgeHeap) Push(x interface{}) {
	*h = append(*h, x.(edge))
}

func (h *edgeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func NewGraphIndex(space MetricSpace) *graph {
	g := &graph{
		checked: make(map[pair]bool),
		space:   space,
	}

	g.gradientDescentKnn(50)
	return g
}

//lint:ignore U1000 .
func (g *graph) dump(k int) {
	n := g.space.Length()
	for i := 0; i < n; i++ {
		sort.Sort(sort.Reverse(g.heaps[i]))
		for j := 0; j < k; j++ {
			edge := g.heaps[i][j]
			fmt.Printf("%v %v %v\n", g.space.At(i), g.space.At(edge.index), edge.distance)
		}
	}
}

func (g *graph) randomize(k int) {
	n := g.space.Length()
	g.heaps = make([]edgeHeap, n)
	g.locks = make([]sync.Mutex, n)

	c := NewCounter(1000)
	fork(n, func(u int) {
		c.Count()
		var v int
		for x := 0; x < k; x++ {
			for {
				v = rand.Int() % n
				if v != u {
					break
				}
			}
			g.connect(v, u, k)
		}
	})
}

/*
func (g *graph) bruteForce(k int) {
	n := g.space.Length()
	g.heaps = make([]edgeHeap, n, n)
	g.locks = make([]sync.Mutex, n, n)

	c := NewCounter(1)
	fork(n, func(u int) {
		c.Count()
		for v := u + 1; v < n; v++ {
			g.connect(u, v, k)
		}
	})
}
*/

func (g *graph) connect(a, b, k int) int {
	g.lock.RLock()
	if g.checked[pair{a, b}] || g.checked[pair{b, a}] || a == b {
		g.lock.RUnlock()
		return 0
	}
	g.lock.RUnlock()

	c := 0
	l := g.space.Distance(g.space.At(a), g.space.At(b))

	g.locks[a].Lock()
	if g.heaps[a].Len() < k {
		heap.Push(&g.heaps[a], edge{b, l, true})
		c++
	} else if l < g.heaps[a][0].distance {
		heap.Pop(&g.heaps[a])
		heap.Push(&g.heaps[a], edge{b, l, true})
		c++
	}

	g.locks[a].Unlock()
	g.locks[b].Lock()

	if g.heaps[b].Len() < k {
		heap.Push(&g.heaps[b], edge{a, l, true})
		c++
	} else if l < g.heaps[b][0].distance {
		heap.Pop(&g.heaps[b])
		heap.Push(&g.heaps[b], edge{a, l, true})
		c++
	}

	g.locks[b].Unlock()

	if c != 0 {
		g.lock.Lock()
		if len(g.checked) >= 1000000 {
			g.checked = make(map[pair]bool)
		}
		g.checked[pair{a, b}] = true
		g.lock.Unlock()
	}

	return c
}

func (g *graph) descentStep(k int, maxSample int, iter int) int {
	// keep track of checked pairs
	g.checked = make(map[pair]bool)

	// find reverse graph
	n := g.space.Length()
	rev := make([][]edge, n)

	for u := 0; u < n; u++ {
		for _, e := range g.heaps[u] {
			rev[e.index] = append(rev[e.index], edge{u, e.distance, e.mark})
			g.checked[pair{u, e.index}] = true
		}
	}

	// for each node,
	c := 0
	counter := NewCounter(100)
	fork(n, func(u int) {
		counter.Count()

		// find lists of old neighbours, new neighbours
		var old []int
		var new []int
		have := make(map[int]bool)

		g.locks[u].Lock()

		for i := range g.heaps[u] {
			e := &g.heaps[u][i]
			if e.mark {
				new = append(new, e.index)
				e.mark = false
			} else {
				old = append(old, e.index)
			}
			have[e.index] = true
		}

		g.locks[u].Unlock()

		odds := float64(maxSample) / float64(len(rev[u]))
		for _, e := range rev[u] {
			if rand.Float64() > odds || have[e.index] {

			} else if e.mark {
				new = append(new, e.index)
			} else {
				old = append(old, e.index)
			}
		}

		//log.Printf("new: %v\nold: %v odds: %v", new, old, odds)
		for i := 0; i < len(new); i++ {
			v := new[i]
			for j := i + 1; j < len(new); j++ {
				w := new[j]
				c += g.connect(v, w, k)
			}

			for _, w := range old {
				if v != w {
					c += g.connect(v, w, k)
				}
			}
		}
	})

	for i := 0; i < 10; i++ {
		runtime.GC()
	}

	return c
}

func fork(n int, fn func(i int)) {
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

func (g *graph) gradientDescentKnn(kIn int) {
	log.Printf("Initialize randomly")
	k := 50
	g.randomize(k)

	//for {
	iter := 1
	for {
		log.Printf("Iteration %v                  ", iter)
		iter++
		c := g.descentStep(k, k, iter)
		if c == 0 {
			break
		}
		log.Printf("%v changes made", c)
	}

	/*
		// optional post stage to find other connections missed.
			// repeat up to 1000 times:
			c2 := 0
			n := g.space.Length()
			log.Printf("Post stage")
			counter := NewCounter(100000)
			fork(n*k*4, func(i int) {
				counter.Count()
				u := rand.Int() % n
				v := rand.Int() % n
				c2 += g.connect(u, v, k)
			})

			log.Printf("Found %v connections during post stage", c2)
			if c2 == 0 {
				break
			}

			iter = 1
			for {
				log.Printf("Iteration %v                  ", iter)
				iter++
				c := g.descentStep(k, k, iter)
				if c == 0 {
					break
				}
				log.Printf("%v changes made", c)
			}
	*/
	//}
}

func (g *graph) GetNodeCount() int {
	return g.space.Length()
}

func (g *graph) GetNeighbours(index int) []edge {
	return g.heaps[index]
}

func (g *graph) GetNode(index int) Point {
	return g.space.At(index)
}

type IGraph interface {
	GetNodeCount() int
	GetNeighbours(index int) []edge
	GetNode(index int) string
}

func randomSample(n, k int) []int {
	have := make(map[int]bool)
	var results []int

	for len(results) < k {
		choice := rand.Intn(n)
		if _, ok := have[choice]; !ok {
			have[choice] = true
			results = append(results, choice)
		}
	}
	return results
}

func (g *graph) NearestNeighbours(target Point, k int) []PointDistance {
	// maintain a heap of the nearest neighbours and a queue of nodes to check.
	var bestSoFar pointHeap
	var worklist []int
	have := make(map[int]bool)

	// things only go onto the heap if they are less distance than the worst
	// when things go onto the heap, they also are added to the queue.

	// find about 100 random neighbours and add to heap (and queue)
	worklist = randomSample(g.space.Length(), 100)
	for _, index := range worklist {
		pt := g.space.At(index)
		have[index] = true
		heap.Push(&bestSoFar, PointDistance{
			Index:    index,
			Point:    pt,
			Distance: g.space.Distance(pt, target),
		})
	}

	// while there are items in the queue,
	for len(worklist) > 0 {
		// take item off the queue
		l := len(worklist)
		index := worklist[l-1]
		worklist = worklist[:l-1]

		// check each of its neighbours.
		for _, edge := range g.GetNeighbours(index) {
			if have[edge.index] {
				continue
			}
			have[edge.index] = true

			// if neighbour needs to go onto the heap, then add it
			d := g.space.Distance(target, g.space.At(edge.index))
			if d < bestSoFar[0].Distance {
				heap.Pop(&bestSoFar)
				heap.Push(&bestSoFar, PointDistance{
					Distance: d,
					Index:    edge.index,
					Point:    g.space.At(edge.index),
				})
				worklist = append(worklist, edge.index)
			}
		}
	}

	sort.Slice(bestSoFar, func(a, b int) bool {
		return bestSoFar[a].Distance < bestSoFar[b].Distance
	})

	log.Printf("Searched %v%% of graph",
		float64(len(have))/float64(g.space.Length()))
	return bestSoFar[:k]
}

func writeNumber(w io.Writer, num uint64, bits int) {
	var buff []byte
	bits = (bits + 7) / 8 * 8
	for i := 0; i < bits; i += 8 {
		buff = append(buff, byte(num>>(bits-8-i)))
	}

	w.Write(buff)
}

func readNumber(r io.Reader, num *uint64, bits int) {
	data := make([]byte, (bits+7)/8)
	r.Read(data)
	*num = 0
	for _, ch := range data {
		*num = (*num << 8) | uint64(ch)
	}
}

func (g *graph) Write(w io.Writer) (int64, error) {
	// write #neighbours
	binary.Write(w, binary.LittleEndian, uint64(len(g.heaps[0])))

	// calc # bits required for node indicies
	b := bits.Len(uint(g.space.Length()))

	// for each node,
	for i := 0; i < len(g.heaps); i++ {
		// write out its neighbours and distances.
		for _, edge := range g.heaps[i] {
			writeNumber(w, uint64(edge.index), b)
			binary.Write(w, binary.LittleEndian, edge.distance)
		}
	}

	// size is 4 + #nodes*#bits + #nodes*float64
	return 0, nil
}

func (g *graph) Save(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()
	g.Write(bw)
}

func ReadGraphIndex(r io.Reader, space MetricSpace) SpaceIndex {

	// read # neighbours
	var k uint64
	binary.Read(r, binary.LittleEndian, &k)

	heaps := make([]edgeHeap, space.Length())

	// calc # bits required for node indicies
	b := bits.Len(uint(space.Length()))

	for i := 0; i < space.Length(); i++ {
		heaps[i] = make(edgeHeap, k)
		for j := uint64(0); j < k; j++ {
			var num uint64
			readNumber(r, &num, b)
			heaps[i][j].index = int(num)
			binary.Read(r, binary.LittleEndian, &heaps[i][j].distance)
		}
	}

	return &graph{
		space: space,
		heaps: heaps,
	}
}

func LoadGraphIndex(filename string, space MetricSpace) SpaceIndex {
	file, err := os.Open(filename)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	br := bufio.NewReader(file)
	return ReadGraphIndex(br, space)
}
