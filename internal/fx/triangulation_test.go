package fx

import "testing"

func TestTriangulationNormalSpreadDoesNotHalt(t *testing.T) {
	result, err := CheckTriangulation(PairRate{"USD", "XLM", 10.1}, PairRate{"USD", "EUR", 1}, PairRate{"EUR", "XLM", 10}, .012)
	if err != nil || result.Halt {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
