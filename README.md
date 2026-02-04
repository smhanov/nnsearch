# nnsearch

[![Go Reference](https://pkg.go.dev/badge/github.com/smhanov/nnsearch.svg)](https://pkg.go.dev/github.com/smhanov/nnsearch)
[![Go Report Card](https://goreportcard.com/badge/github.com/smhanov/nnsearch)](https://goreportcard.com/report/github.com/smhanov/nnsearch)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Fast nearest neighbour search in arbitrary metric spaces for Go.**

nnsearch is a high-performance Go library for finding similar items in any metric space using the graph-based descent method. Unlike vector-only solutions, nnsearch works with *any* distance function — from edit distance on strings to custom similarity measures on complex objects.

## 🎯 Key Features

- **Works with any metric space** — not just vectors! Use edit distance, Jaccard similarity, custom distance functions, and more
- **Graph-based descent method** — achieves sub-linear query time after index construction  
- **Pivot-based initialization** — smart index bootstrapping using landmark points
- **Parallel processing** — leverages all CPU cores for index building and querying
- **Persistent indexes** — save and load indexes using memory-mapped files
- **Filter support** — exclude items during search without rebuilding the index
- **Brute-force fallback** — simple baseline for comparison and small datasets

## 📚 Background: Nearest Neighbour Search in Metric Spaces

### What is a Metric Space?

A **metric space** is any set of objects with a distance function that satisfies:

1. **Identity**: `d(x, x) = 0`
2. **Positivity**: `d(x, y) > 0` when `x ≠ y`
3. **Symmetry**: `d(x, y) = d(y, x)`
4. **Triangle inequality**: `d(x, z) ≤ d(x, y) + d(y, z)`

This is more general than the familiar Euclidean space. Examples include:

| Data Type | Distance Function |
|-----------|-------------------|
| Strings | Levenshtein (edit) distance |
| Sets | Jaccard distance |
| Word embeddings | Cosine distance |
| Time series | Dynamic Time Warping |
| Graphs | Graph edit distance |
| DNA sequences | Hamming distance |

### The Challenge

Given a query point and a collection of items, find the k items closest to the query. With millions of items, brute-force comparison is too slow.

Traditional tree-based structures (KD-trees, Ball trees) work well in low dimensions but suffer from the **curse of dimensionality** — as dimensions increase, performance degrades to brute-force.

### The Solution: Graph-Based Descent

nnsearch uses a **proximity graph** approach:

1. **Index Construction**: Build a graph where each node connects to its approximate k-nearest neighbours
2. **Query**: Starting from random entry points, "walk" through the graph toward the query point, always moving to neighbours closer to the target

This achieves **sub-linear** query time in practice, even in high dimensions.

## 🔬 How It Works

### Index Construction

nnsearch builds the proximity graph using the **NN-Descent** algorithm:

1. **Pivot Selection**: Choose landmark points that maximize coverage of the space
2. **Initial Connections**: Use pivot-based hashing to connect nearby points
3. **Local Optimization**: Iteratively improve connections — if A→B and A→C, check if B→C improves either's neighbourhood
4. **Random Exploration**: Add random edges to escape local minima
5. **Make Undirected**: Ensure bidirectional connectivity for robust traversal

### Query Algorithm

```
1. Start at random nodes in the graph
2. Maintain a priority queue of candidates ordered by distance to query
3. For each candidate, examine its neighbours
4. If a neighbour is closer to the query, add it to candidates
5. Stop when no improvement is found
6. Return the k closest points seen
```

The graph structure means we typically examine only a small fraction of points (often <5%) to find exact or near-exact results.

## 📦 Installation

```bash
go get github.com/smhanov/nnsearch
```

## 🚀 Quick Start

### Basic Example: Custom Distance Function

```go
package main

import (
    "fmt"
    "github.com/smhanov/nnsearch"
)

// StringSpace implements MetricSpace for strings with edit distance
type StringSpace struct {
    words []string
}

func (s *StringSpace) Length() int {
    return len(s.words)
}

func (s *StringSpace) At(i int) nnsearch.Point {
    return s.words[i]
}

func (s *StringSpace) Distance(p1, p2 nnsearch.Point) float64 {
    return float64(editDistance(p1.(string), p2.(string)))
}

func editDistance(s1, s2 string) int {
    // ... implementation ...
}

func main() {
    // Create your metric space
    space := &StringSpace{
        words: []string{"apple", "apply", "maple", "application", ...},
    }
    
    // Build the index (this takes time but only done once)
    index := nnsearch.NewGraphIndex(space)
    
    // Find 5 nearest neighbours to "appel" (misspelling)
    results := index.NearestNeighbours("appel", 5, nil)
    
    for _, r := range results {
        fmt.Printf("%s (distance: %.0f)\n", r.Point, r.Distance)
    }
}
```

### Using Word Vectors

```go
// Load word vectors (word2vec format)
wv := nnsearch.OpenWordVecs("vectors.bin")

// Build index
index := nnsearch.NewGraphIndex(wv)

// Find similar words
query := wv.PointFromQuery("king")
results := index.NearestNeighbours(query, 10, nil)
```

### Saving and Loading Indexes

```go
// Build and save
index := nnsearch.NewGraphIndex(space)
index.Save("myindex.bin")

// Load later (much faster than rebuilding)
index, err := nnsearch.LoadGraphIndex("myindex.bin", space)
```

### Filtering Results

```go
// Only return words starting with 'a'
options := &nnsearch.SearchOptions{
    Filter: func(pt nnsearch.Point) bool {
        return strings.HasPrefix(pt.(string), "a")
    },
}

results := index.NearestNeighbours(query, 10, options)
```

### Context for Cancellation

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

options := &nnsearch.SearchOptions{
    Ctx: ctx,
}

results := index.NearestNeighbours(query, 10, options)
```

## 📖 API Reference

### Core Interfaces

#### `MetricSpace`

Implement this interface to define your data and distance function:

```go
type MetricSpace interface {
    Length() int                        // Number of points
    At(i int) Point                     // Get point at index
    Distance(p1, p2 Point) float64      // Distance between points
}
```

#### `SpaceIndex`

Index interface for nearest neighbour queries:

```go
type SpaceIndex interface {
    MetricSpace
    NearestNeighbours(target Point, k int, options *SearchOptions) []PointDistance
    Write(w io.Writer) (int64, error)
}
```

### Index Types

| Constructor | Description | Use Case |
|-------------|-------------|----------|
| `NewGraphIndex(space)` | Graph-based descent index | Large datasets, any metric |
| `NewBruteForceIndex(space)` | Linear scan | Small datasets, baseline comparison |

### Utility Functions

```go
// Search multiple indexes in parallel
SearchAll(target, k, options, indices...)

// Compute average/median distance for calibration
ComputeAverageDistance(space, samples)
ComputeMedianDistance(space, samples)

// Distance functions
EuclideanDistance(vec1, vec2 []float32) float64
CosineDistance(vec1, vec2 []float32) float64
```

## 🎨 Use Cases

### Spell Checking & Correction

Find the closest valid words to a misspelled input using edit distance.

### Fuzzy Search

Match user queries against a database of names, products, or documents even with typos.

### Recommendation Systems

Find similar items (movies, products, articles) based on feature vectors or user behaviour embeddings.

### Plagiarism Detection

Detect similar documents using document embeddings or n-gram distance.

### Deduplication

Find near-duplicate records in databases using custom similarity functions.

### Semantic Search

Use word embeddings (Word2Vec, GloVe, fastText) to find semantically similar text.

### Bioinformatics

Search for similar DNA/protein sequences using sequence alignment distances.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      User's MetricSpace                      │
│         (implements Length, At, Distance)                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Index Construction                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Pivots    │→ │   Initial   │→ │    NN-Descent       │  │
│  │  Selection  │  │   Graph     │  │    Iterations       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Proximity Graph Index                     │
│              (serializable via FrozenFile)                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Query Processing                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Random    │→ │   Graph     │→ │    Return Top-K     │  │
│  │   Entry     │  │   Descent   │  │    Results          │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## ⚡ Performance Tips

1. **Batch Index Building**: Construction is parallelized across all CPU cores
2. **Persist Your Index**: Use `Save()` / `LoadGraphIndex()` to avoid rebuilding
3. **Memory-Mapped Files**: Large indexes are loaded efficiently via mmap
4. **Tune the Entry Points**: The algorithm starts from 10 random points by default
5. **Pre-compute What You Can**: If distances are expensive, consider caching

## 🔧 Advanced: Implementing MetricSpace

### Example: Jaccard Distance for Sets

```go
type SetSpace struct {
    sets []map[string]bool
}

func (s *SetSpace) Length() int {
    return len(s.sets)
}

func (s *SetSpace) At(i int) nnsearch.Point {
    return s.sets[i]
}

func (s *SetSpace) Distance(p1, p2 nnsearch.Point) float64 {
    a := p1.(map[string]bool)
    b := p2.(map[string]bool)
    
    intersection := 0
    for k := range a {
        if b[k] {
            intersection++
        }
    }
    
    union := len(a) + len(b) - intersection
    if union == 0 {
        return 0
    }
    return 1.0 - float64(intersection)/float64(union)
}
```

### Example: Time Series with DTW

```go
type TimeSeriesSpace struct {
    series [][]float64
}

func (t *TimeSeriesSpace) Distance(p1, p2 nnsearch.Point) float64 {
    return dynamicTimeWarping(p1.([]float64), p2.([]float64))
}
```

## 📊 Benchmarks

Performance depends heavily on:
- Size of dataset
- Dimensionality / complexity of distance function
- Intrinsic dimensionality of data

Typical results on 100K word vectors:

| Method | Query Time | Recall@10 |
|--------|-----------|-----------|
| Brute Force | ~50ms | 100% |
| Graph Descent | ~2ms | 98%+ |

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- The NN-Descent algorithm is based on the paper ["Efficient K-Nearest Neighbor Graph Construction for Generic Similarity Measures"](http://www.cs.princeton.edu/cass/papers/www11.pdf) by Dong, Moses, and Li.
- Pivot-based methods inspired by work on [LAESA](https://doi.org/10.1016/0167-8655(94)90095-7) and related algorithms.

## 📫 Contact

For questions, issues, or suggestions, please [open an issue](https://github.com/smhanov/nnsearch/issues) on GitHub.

---

⭐ If you find this library useful, please consider giving it a star on GitHub!
