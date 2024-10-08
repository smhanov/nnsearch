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
	MetricSpace
	Heaps   []edgeHeap
	Checked map[pair]bool
	lock    sync.RWMutex
	Locks   []sync.Mutex
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
		MetricSpace: space,
		Checked:     make(map[pair]bool),
	}

	n := g.Length()
	g.Heaps = make([]edgeHeap, n)
	g.Locks = make([]sync.Mutex, n)

	g.gradientDescentKnn(50)
	return g
}

//lint:ignore U1000 .
func (g *graph) dump(k int) {
	n := g.Length()
	for i := 0; i < n; i++ {
		sort.Sort(sort.Reverse(g.Heaps[i]))
		for j := 0; j < k; j++ {
			edge := g.Heaps[i][j]
			fmt.Printf("%v %v %v\n", g.At(i), g.At(edge.index), edge.distance)
		}
	}
}

func (g *graph) randomize(k int) {
	c := NewCounter(1000)
	n := g.Length()
	ForkLoop(g.Length(), func(u int) {
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

func (g *graph) initializeUsingPivots(pivots Pivots, k int) {
	n := g.Length()
	if n < 1 {
		return
	}

	if k > n-1 {
		k = n - 1
	}

	hashes := make([][]int, n)
	for u := 0; u < n; u++ {
		hashes[u] = pivots.Hash(u)
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
	if g.Checked[pair{a, b}] || g.Checked[pair{b, a}] || a == b {
		g.lock.RUnlock()
		return 0
	}
	g.lock.RUnlock()

	c := 0
	l := g.Distance(g.At(a), g.At(b))

	g.Locks[a].Lock()
	if g.Heaps[a].Len() < k {
		heap.Push(&g.Heaps[a], edge{b, l, true})
		c++
	} else if l < g.Heaps[a][0].distance {
		heap.Pop(&g.Heaps[a])
		heap.Push(&g.Heaps[a], edge{b, l, true})
		c++
	}

	g.Locks[a].Unlock()
	g.Locks[b].Lock()

	if g.Heaps[b].Len() < k {
		heap.Push(&g.Heaps[b], edge{a, l, true})
		c++
	} else if l < g.Heaps[b][0].distance {
		heap.Pop(&g.Heaps[b])
		heap.Push(&g.Heaps[b], edge{a, l, true})
		c++
	}

	g.Locks[b].Unlock()

	if c != 0 {
		g.lock.Lock()
		if len(g.Checked) >= 1000000 {
			g.Checked = make(map[pair]bool)
		}
		g.Checked[pair{a, b}] = true
		g.lock.Unlock()
	}

	return c
}

func (g *graph) descentStep(k int, maxSample int, iter int) int {
	// keep track of checked pairs
	g.Checked = make(map[pair]bool)

	// find reverse graph
	n := g.Length()
	rev := make([][]edge, n)

	for u := 0; u < n; u++ {
		for _, e := range g.Heaps[u] {
			rev[e.index] = append(rev[e.index], edge{u, e.distance, e.mark})
			g.Checked[pair{u, e.index}] = true
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

		g.Locks[u].Lock()

		for i := range g.Heaps[u] {
			e := &g.Heaps[u][i]
			if e.mark {
				new = append(new, e.index)
				e.mark = false
			} else {
				old = append(old, e.index)
			}
			have[e.index] = true
		}

		g.Locks[u].Unlock()

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
	n := g.Length()
	np := int(math.Max(math.Log2(float64(n)), 3))
	log.Printf("Choosing %d pivots", np)
	pivots := ChoosePivots(g)

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
	return g.Length()
}

func (g *graph) GetNeighbours(index int) []edge {
	return g.Heaps[index]
}

func (g *graph) GetNode(index int) Point {
	return g.At(index)
}

func (g *graph) NearestNeighbours(target Point, k int, options *SearchOptions) []PointDistance {
	return NearestNeighbours(g, target, k, options)
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
	n := g.Length()
	redges := make([]edgeHeap, n)

	// create heaps to contain the reverse edges
	for u := 0; u < n; u++ {
		for _, edge := range g.Heaps[u] {
			have[pair{u, edge.index}] = true
		}
	}

	// add all reverse edges to nodes' neighbour list, up to the limit of k.
	for u := 0; u < n; u++ {
		for _, e := range g.Heaps[u] {
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
			heap.Push(&g.Heaps[u], e)
		}

		for len(redges[u]) > k {
			heap.Pop(&g.Heaps[u])
		}
	}
}

type frozenGraph struct {
	MetricSpace
	ff *FrozenFile
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

func (g *frozenGraph) NearestNeighbours(target Point, k int, options *SearchOptions) []PointDistance {
	return NearestNeighbours(g, target, k, options)
}

func (g *frozenGraph) GetNode(index int) Point {
	return g.At(index)
}

func (g *frozenGraph) Write(w io.Writer) (int64, error) {
	return 0, fmt.Errorf("cannot write frozen graph")
}

type IGraph interface {
	MetricSpace
	GetNodeCount() int
	GetNeighbours(index int) []edge
	GetNode(index int) Point
}

/*
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
*/

func NearestNeighbours(g IGraph, target Point, k int, optionsIn *SearchOptions) []PointDistance {
	opt := getOptions(optionsIn)
	space := g
	var bestk pointHeap
	var queue minEdgeHeap
	epsilon := 1.1
	checked := make(map[int]bool)
	n := space.Length()

	var mutex sync.Mutex
	gthreshold := math.Inf(1)

	consider := func(u int) bool {
		mutex.Lock()
		if checked[u] {
			mutex.Unlock()
			return false
		}

		checked[u] = true
		mutex.Unlock()

		pt := space.At(u)
		d := space.Distance(pt, target)

		mutex.Lock()
		defer mutex.Unlock()
		if (len(bestk) < k || d < bestk[0].Distance) && opt.Filter(pt) {
			if len(bestk) == k {
				heap.Pop(&bestk)
			}
			heap.Push(&bestk, PointDistance{
				Distance: d,
				Index:    u,
				Point:    pt,
			})
			gthreshold = epsilon * d
		}

		heap.Push(&queue, edge{u, d, false})
		return true
	}

	found := 0
	for found < 10 {
		if consider(rand.Intn(n)) {
			found++
		}
	}

	ForkWhile(func() bool {
		if opt.Ctx.Err() != nil {
			return false
		}
		mutex.Lock()
		if len(queue) == 0 {
			mutex.Unlock()
			return false
		}

		item := heap.Pop(&queue).(edge)
		if len(bestk) == k && item.distance > gthreshold {
			mutex.Unlock()
			return false
		}
		mutex.Unlock()
		for _, e := range g.GetNeighbours(item.index) {
			consider(e.index)
		}
		return true
	})

	sort.Slice(bestk, func(a, b int) bool {
		if bestk[a].Distance < bestk[b].Distance {
			return true
		} else if bestk[a].Distance > bestk[b].Distance {
			return false
		}
		return bestk[a].Index > bestk[b].Index
	})

	log.Printf("Searched %.1f%% of graph",
		float64(len(checked))/float64(g.GetNodeCount())*100)
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
	items := make([]FrozenItem, len(g.Heaps))
	for i := range g.Heaps {
		items[i] = &g.Heaps[i]
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

func LoadGraphIndex(filename string, space MetricSpace) (SpaceIndex, error) {
	ff, err := OpenFrozenFile(filename)
	if err != nil {
		return nil, err
	}
	return &frozenGraph{
		MetricSpace: space,
		ff:          ff,
	}, nil
}
