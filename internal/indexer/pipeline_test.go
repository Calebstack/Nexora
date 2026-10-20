package indexer

import "testing"

type source struct{ calls int }
func (s *source) Operations(_, _ uint32) ([]Operation, error) { s.calls++; return []Operation{{Hash: "tx", Ledger: 1, TenantID: "t", BalanceDelta: 4}}, nil }
type store struct{ seen map[string]bool }
func (s *store) InsertOperation(_ Operation, key string) (bool, error) { if s.seen[key] { return false, nil }; s.seen[key] = true; return true, nil }
func TestPipelineDeduplicates(t *testing.T) { src := &source{}; count, err := (Pipeline{Source: src, Store: &store{seen: map[string]bool{}}}).Ingest(1, 50); if err != nil || count != 1 || src.calls != 1 { t.Fatalf("count=%d calls=%d err=%v", count, src.calls, err) } }
