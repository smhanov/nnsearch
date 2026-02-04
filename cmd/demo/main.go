package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/smhanov/nnsearch"
)

// StringSpace implements nnsearch.MetricSpace for strings using edit distance
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

// editDistance computes the Levenshtein distance between two strings
func editDistance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Use two rows to save memory
	prev := make([]int, len(s2)+1)
	curr := make([]int, len(s2)+1)

	// Initialize first row
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(s1); i++ {
		curr[0] = i
		for j := 1; j <= len(s2); j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}
			curr[j] = min3(
				prev[j]+1,      // deletion
				curr[j-1]+1,    // insertion
				prev[j-1]+cost, // substitution
			)
		}
		prev, curr = curr, prev
	}

	return prev[len(s2)]
}

func min3(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= c {
		return b
	}
	return c
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// loadDictionary loads words from a file (one word per line)
func loadDictionary(path string, maxWords int) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var words []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		word := strings.TrimSpace(scanner.Text())
		// Skip empty lines, comments, and words with apostrophes or special chars
		if word == "" || strings.HasPrefix(word, "#") {
			continue
		}
		// Only include alphabetic words
		valid := true
		for _, r := range word {
			if r < 'a' || r > 'z' {
				if r < 'A' || r > 'Z' {
					valid = false
					break
				}
			}
		}
		if valid && len(word) > 1 {
			words = append(words, strings.ToLower(word))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// If maxWords is set, randomly sample instead of taking first N
	// This ensures we get a representative sample from the whole dictionary
	if maxWords > 0 && len(words) > maxWords {
		// Shuffle and take first maxWords
		rand.Shuffle(len(words), func(i, j int) {
			words[i], words[j] = words[j], words[i]
		})
		words = words[:maxWords]
	}

	return words, nil
}

func main() {
	// Command line flags
	dictPath := flag.String("dict", "/usr/share/dict/words", "Path to dictionary file")
	k := flag.Int("k", 5, "Number of nearest neighbours to find")
	maxWords := flag.Int("max", 0, "Maximum words to load (0 = all)")
	indexFile := flag.String("index", "", "Path to save/load index (optional)")
	bruteForce := flag.Bool("brute", false, "Use brute force instead of graph index")
	interactive := flag.Bool("i", false, "Interactive mode")
	flag.Parse()

	// Get query words from remaining arguments
	queries := flag.Args()

	if len(queries) == 0 && !*interactive {
		fmt.Println("nnsearch demo - Find similar words using edit distance")
		fmt.Println()
		fmt.Println("Usage: demo [options] <word> [word2] [word3] ...")
		fmt.Println()
		fmt.Println("Options:")
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  demo apple                    # Find words similar to 'apple'")
		fmt.Println("  demo -k 10 helo wrold         # Find 10 similar words for each")
		fmt.Println("  demo -i                       # Interactive mode")
		fmt.Println("  demo -brute apple             # Compare with brute force")
		fmt.Println("  demo -index words.idx apple   # Save/load index for faster startup")
		os.Exit(0)
	}

	// Load dictionary
	fmt.Printf("📖 Loading dictionary from %s...\n", *dictPath)
	start := time.Now()
	words, err := loadDictionary(*dictPath, *maxWords)
	if err != nil {
		log.Fatalf("Failed to load dictionary: %v", err)
	}
	fmt.Printf("   Loaded %d words in %v\n", len(words), time.Since(start))

	// Remove duplicates and sort
	wordSet := make(map[string]bool)
	for _, w := range words {
		wordSet[w] = true
	}
	words = make([]string, 0, len(wordSet))
	for w := range wordSet {
		words = append(words, w)
	}
	sort.Strings(words)
	fmt.Printf("   %d unique words after deduplication\n", len(words))

	// Create metric space
	space := &StringSpace{words: words}

	// Build or load index
	var index nnsearch.SpaceIndex

	if *bruteForce {
		fmt.Println("\n🔨 Using brute force search (linear scan)")
		index = nnsearch.NewBruteForceIndex(space)
	} else if *indexFile != "" {
		// Try to load existing index
		if _, err := os.Stat(*indexFile); err == nil {
			fmt.Printf("\n📂 Loading index from %s...\n", *indexFile)
			start = time.Now()
			loadedIndex, err := nnsearch.LoadGraphIndex(*indexFile, space)
			if err != nil {
				log.Printf("   Failed to load index: %v, will rebuild", err)
			} else {
				index = loadedIndex
				fmt.Printf("   Loaded in %v\n", time.Since(start))
			}
		}
	}

	if index == nil && !*bruteForce {
		// Build new index
		fmt.Println("\n🏗️  Building graph index (this may take a while for large dictionaries)...")
		start = time.Now()
		graphIndex := nnsearch.NewGraphIndex(space)
		index = graphIndex
		fmt.Printf("   Built in %v\n", time.Since(start))

		// Save if path specified
		if *indexFile != "" {
			fmt.Printf("💾 Saving index to %s...\n", *indexFile)
			graphIndex.Save(*indexFile)
		}
	}

	// Function to search and display results
	search := func(query string) {
		fmt.Printf("\n🔍 Searching for words similar to '%s'...\n", query)
		start := time.Now()
		results := index.NearestNeighbours(query, *k, nil)
		elapsed := time.Since(start)

		if len(results) == 0 {
			fmt.Println("   No results found")
			return
		}

		fmt.Printf("\n   Top %d matches (in %v):\n", len(results), elapsed)
		fmt.Println("   ─────────────────────────────────")
		for i, r := range results {
			word := r.Point.(string)
			dist := int(r.Distance)
			bar := strings.Repeat("█", max(0, 10-dist)) + strings.Repeat("░", minInt(10, dist))
			fmt.Printf("   %2d. %-20s  dist: %d  %s\n", i+1, word, dist, bar)
		}
	}

	// Process queries
	if *interactive {
		fmt.Println("\n🎮 Interactive mode - type a word to find similar words, or 'quit' to exit")
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("\n> ")
			if !scanner.Scan() {
				break
			}
			query := strings.TrimSpace(scanner.Text())
			if query == "" {
				continue
			}
			if query == "quit" || query == "exit" || query == "q" {
				fmt.Println("Goodbye! 👋")
				break
			}
			search(query)
		}
	} else {
		for _, query := range queries {
			search(query)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
