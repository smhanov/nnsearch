package nnsearch

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"math"
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

type minEdgeHeap []edge

func (h minEdgeHeap) Len() int           { return len(h) }
func (h minEdgeHeap) Less(i, j int) bool { return h[i].distance < h[j].distance }
func (h minEdgeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minEdgeHeap) Push(x interface{}) {
	*h = append(*h, x.(edge))
}
func (h *minEdgeHeap) Pop() interface{} {
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

	n := g.space.Length()
	g.heaps = make([]edgeHeap, n)
	g.locks = make([]sync.Mutex, n)

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
	c := NewCounter(1000)
	n := g.space.Length()
	ForkLoop(g.space.Length(), func(u int) {
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

func (g *graph) initializeUsingPivots(pivots []Pivot, k int) {
	n := g.space.Length()
	if n < 1 {
		return
	}

	if k > n-1 {
		k = n - 1
	}

	hashes := make([][]int, n)
	for u := 0; u < n; u++ {
		hashes[u] = PivotHash(pivots, u)
	}

	order := Sequence(n)
	sort.Slice(order, func(a, b int) bool {
		return PivotHashLessThan(hashes[a], hashes[b])
	})

	c := NewCounter(100)
	ShuffledForkLoop(n, func(i int) {
		c.Count()
		u := order[i]
		start := i - k/2
		if start < 0 {
			start = 0
		}

		end := start + k + 1
		if end > n {
			end = n
		}

		for j := start; j < end; j++ {
			if i != j {
				v := order[j]
				g.connect(u, v, k)
			}
		}
	})

	g.randomize(k)
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
	ForkLoop(n, func(u int) {
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

func (g *graph) gradientDescentKnn(kIn int) {
	n := g.space.Length()
	np := int(math.Max(math.Log2(float64(n)), 3))
	log.Printf("Choosing %d pivots", np)
	pivots := ChoosePivots(g.space, np)

	k := 50
	log.Printf("Initialize using pivots")
	g.initializeUsingPivots(pivots, k)

	log.Printf("Adding randomness")
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

	g.makeUndirected(k * 2)

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

func (g *graph) NearestNeighbours(target Point, k int, filter PointFilter) []PointDistance {
	return NearestNeighbours(g, target, k, filter)
}

func (g *graph) Space() MetricSpace {
	return g.space
}

/*
func pushk(h heap.Interface, x interface{}, k int) {
	if h.Len() < k {
		heap.Push(h, x)
		return
	}

	h.Push(x)
	less := h.Less(h.Len()-1, 0)
	h.Pop()
	if less {
		heap.Pop(h)
		heap.Push(h, x)
	}
}
*/

func (g *graph) makeUndirected(k int) {
	have := make(map[pair]bool)
	// check what edges we have already
	n := g.space.Length()
	redges := make([]edgeHeap, n)

	// create heaps to contain the reverse edges
	for u := 0; u < n; u++ {
		for _, edge := range g.heaps[u] {
			have[pair{u, edge.index}] = true
		}
	}

	// add all reverse edges to nodes' neighbour list, up to the limit of k.
	for u := 0; u < n; u++ {
		for _, e := range g.heaps[u] {
			if !have[pair{e.index, u}] {
				have[pair{e.index, u}] = true
			}
			l := len(redges[e.index])
			if l < k || e.distance < redges[e.index][0].distance {
				if l == k {
					heap.Pop(&redges[e.index])
				}
				heap.Push(&redges[e.index], edge{u, e.distance, false})
			}
		}
	}

	// add all reverse edges to nodes' neighbour list, up to the limit of k.
	for u := 0; u < n; u++ {
		for _, e := range redges[u] {
			heap.Push(&g.heaps[u], e)
		}

		for len(redges[u]) > k {
			heap.Pop(&g.heaps[u])
		}
	}
}

type frozenGraph struct {
	ff    *FrozenFile
	space MetricSpace
}

func (g *frozenGraph) Space() MetricSpace {
	return g.space
}

func (g *frozenGraph) GetNodeCount() int {
	return int(g.ff.GetCount())
}

func (g *frozenGraph) GetNeighbours(index int) []edge {
	var n edgeHeap
	g.ff.GetItem(index, &n)
	return n
}

func (e *edgeHeap) Encode(w io.Writer) uint64 {
	s := WriteThing(w, len(*e))
	for _, edge := range *e {
		s += WriteThing(w, edge.index)
		s += WriteThing(w, edge.distance)
	}
	return s
}

func (e *edgeHeap) Decode(r ByteInputStream) {
	var l int
	ReadThing(r, &l)
	*e = make(edgeHeap, l)
	for i := 0; i < l; i++ {
		ReadThing(r, &(*e)[i].index)
		ReadThing(r, &(*e)[i].distance)
	}
}

func (g *frozenGraph) NearestNeighbours(target Point, k int, filter PointFilter) []PointDistance {
	return NearestNeighbours(g, target, k, filter)
}

func (g *frozenGraph) GetNode(index int) Point {
	return g.space.At(index)
}

func (g *frozenGraph) Write(w io.Writer) (int64, error) {
	return 0, fmt.Errorf("cannot write frozen graph")
}

type IGraph interface {
	Space() MetricSpace
	GetNodeCount() int
	GetNeighbours(index int) []edge
	GetNode(index int) Point
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

func NearestNeighbours(g IGraph, target Point, k int, filter PointFilter) []PointDistance {
	space := g.Space()
	var bestk pointHeap
	var queue minEdgeHeap
	epsilon := 1.1
	checked := make(map[int]bool)
	n := space.Length()

	consider := func(u int) bool {
		if checked[u] {
			return false
		}

		checked[u] = true
		pt := space.At(u)

		d := space.Distance(pt, target)
		if (len(bestk) < k || d < bestk[0].Distance) && filter(pt) {
			if len(bestk) == k {
				heap.Pop(&bestk)
			}
			heap.Push(&bestk, PointDistance{
				Distance: d,
				Index:    u,
				Point:    pt,
			})
		}

		if len(bestk) == 0 || d < epsilon*bestk[0].Distance {
			heap.Push(&queue, edge{u, d, false})
		}
		return true
	}

	found := 0
	for found < 10 {
		if consider(rand.Intn(n)) {
			found++
		}
	}

	for {
		if len(queue) == 0 {
			break
		}

		item := heap.Pop(&queue).(edge)
		for _, e := range g.GetNeighbours(item.index) {
			consider(e.index)
		}
	}

	sort.Slice(bestk, func(a, b int) bool {
		return bestk[a].Distance < bestk[b].Distance
	})

	log.Printf("Searched %v%% of graph",
		float64(len(checked))/float64(g.GetNodeCount()))
	return bestk
}

/*
func writeNumber(w io.Writer, num uint64, bits int) {
	var buff []byte
	bits = (bits + 7) / 8 * 8
	for i := 0; i < bits; i += 8 {
		buff = append(buff, byte(num>>(bits-8-i)))
	}

	_, err := w.Write(buff)
	if err != nil {
		log.Panic(err)
	}
}

func readNumber(r io.Reader, num *uint64, bits int) {
	data := make([]byte, (bits+7)/8)
	_, err := r.Read(data)
	if err != nil {
		log.Panic(err)
	}
	*num = 0
	for _, ch := range data {
		*num = (*num << 8) | uint64(ch)
	}
}
*/

func (g *graph) Write(w io.Writer) (int64, error) {
	items := make([]FrozenItem, len(g.heaps))
	for i := range g.heaps {
		items[i] = &g.heaps[i]
	}
	n := FreezeItems(w, items)
	return int64(n), nil
}

func (g *graph) Save(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()
	_, err = g.Write(bw)
	if err != nil {
		log.Panic(err)
	}
}

func LoadGraphIndex(filename string, space MetricSpace) SpaceIndex {
	return &frozenGraph{
		ff:    OpenFrozenFile(filename),
		space: space,
	}
}
