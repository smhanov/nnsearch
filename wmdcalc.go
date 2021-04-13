package nnsearch

import (
	"log"
	"math"

	"github.com/yizha/go/tp"
)

// Word Mover Distance calculator

type WmdCalc struct {
	wv     *WordVecs
	cutoff float64 // cutoff, the "average" distance between points beyond which it is not useful to keep neighbours
}

func NewWordMoverDistanceCalculator(wv *WordVecs) *WmdCalc {
	return &WmdCalc{wv, ComputeAverageDistance(wv, 1000)}
}

func (wc *WmdCalc) calculateDistanceMatrix(d1, d2 []string) [][]float64 {
	wv1Len, wv2Len := len(d1), len(d2)
	dm := make([][]float64, wv1Len)
	for i := 0; i < wv1Len; i++ {
		dm[i] = make([]float64, wv2Len)
		v1 := wc.wv.Get(d1[i])
		if v1 != nil {
			for j := 0; j < wv2Len; j++ {
				v2 := wc.wv.Get(d2[j])
				if v2 != nil {
					dm[i][j] = CosineDistance(v1, v2)
					//log.Printf("dist is %v, %v is %v", v1, v2, dm[i][j])
				} else {
					dm[i][j] = wc.cutoff
				}
			}
		} else {
			for j := 0; j < wv2Len; j++ {
				dm[i][j] = wc.cutoff
			}
		}
	}
	return dm
}

func ones(count int) []float64 {
	ret := make([]float64, count)
	for i := range ret {
		ret[i] = 1 / float64(count)
	}

	return ret
}

/*
func printmatrix(words1, words2 []string, mat [][]float64) {
	log.Printf("%v %v mat=%v", len(words1), len(words2), mat)
	for i := range words1 {
		for j := range words2 {
			log.Printf("%v/%v: %v", words1[i], words2[j], mat[i][j])
		}
	}
}
*/

func (wc *WmdCalc) Compute(words1, words2 []string) float64 {
	if len(words1) == 0 || len(words2) == 0 {
		return math.Inf(1)
	}

	//log.Printf("%v=>%v", words1, words2)

	dm := wc.calculateDistanceMatrix(words1, words2)
	p, err := tp.CreateProblem(ones(len(words1)), ones(len(words2)), dm)
	if err != nil {
		log.Panic(err)
		return math.Inf(1)
	}
	err = p.Solve()
	if err != nil {
		log.Panic(err)
		return math.Inf(1)
	}

	cost := p.GetCost() /*
		log.Printf("%v(%v)=>%v(%v): %v", words1, ones(len(words1)), words2, ones(len(words2)), cost)
		log.Printf("Distances")
		printmatrix(words1, words2, dm)
		log.Printf("Flow")
		printmatrix(words1, words2, p.GetFlow())*/
	return cost
}

func (wc *WmdCalc) Compute_old(words1, words2 []string) float64 {
	if len(words2) > len(words1) {
		words1, words2 = words2, words1
	}

	var dist float64
	matched := 0
	// for each word in first
	for _, word1 := range words1 {
		if word1 == "" {
			continue
		}
		vec1 := wc.wv.Get(word1)
		if vec1 == nil {
			dist += wc.cutoff
			continue
		}
		// find closest word in second and add to distance.
		closest := wc.cutoff
		//cword := ""
		for _, word2 := range words2 {
			if word2 == "" {
				continue
			}
			if word1 == word2 {
				closest = 0
				//cword = word2
				matched++
				break
			}

			vec2 := wc.wv.Get(word2)
			if vec2 == nil {
				continue
			}

			d := CosineDistance(vec1, vec2)
			if d < closest {
				closest = d
				//cword = word2
				matched++
			}
		}

		//log.Printf("%v=>%v: %v", word1, cword, closest)
		dist += closest
	}

	if matched > 0 {
		//log.Printf("Dist=%v", dist)
		return dist
	}

	return math.Inf(1)
}
