package nnsearch

import (
	"fmt"
	"os"
	"time"
)

// Counter ...
type Counter struct {
	start time.Time
	count int
	freq  int
}

// NewCounter ...
func NewCounter(freq int) *Counter {
	return &Counter{
		start: time.Now(),
		freq:  freq,
	}
}

// Count ...
func (c *Counter) Count() {
	c.count++
	if c.count%c.freq == 0 {
		fmt.Fprintf(os.Stderr, "%d (%.1f items/s)\r", c.count, float64(c.count)/(time.Since(c.start).Seconds()))
	}
}
