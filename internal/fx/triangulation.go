package fx

import "fmt"

type PairRate struct {
	From, To string
	Rate     float64
}
type TriangulationResult struct {
	Direct, Implied, Discrepancy float64
	Halt                         bool
}

func CheckTriangulation(direct PairRate, first, second PairRate, maxDiscrepancy float64) (TriangulationResult, error) {
	if direct.Rate <= 0 || first.Rate <= 0 || second.Rate <= 0 {
		return TriangulationResult{}, fmt.Errorf("rates must be positive")
	}
	implied := first.Rate * second.Rate
	discrepancy := (direct.Rate - implied) / implied
	if discrepancy < 0 {
		discrepancy = -discrepancy
	}
	return TriangulationResult{Direct: direct.Rate, Implied: implied, Discrepancy: discrepancy, Halt: discrepancy > maxDiscrepancy}, nil
}
