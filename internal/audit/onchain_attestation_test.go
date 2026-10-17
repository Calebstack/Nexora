package audit

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
)

type fakeEventSource struct {
	events   []*domain.AuditEvent
	from, to time.Time
	err      error
}

func (f *fakeEventSource) ListBetween(_ context.Context, from, to time.Time) ([]*domain.AuditEvent, error) {
	f.from, f.to = from, to
	return f.events, f.err
}

type fakeCommitter struct {
	calls    int
	failures int
	day      uint64
	root     [32]byte
	metadata string
}

func (f *fakeCommitter) Commit(_ context.Context, day uint64, root [32]byte, metadata string) (string, error) {
	f.calls++
	if f.calls <= f.failures {
		return "", errors.New("rpc unavailable")
	}
	f.day, f.root, f.metadata = day, root, metadata
	return "tx-hash", nil
}

func testLeaves(n int) [][32]byte {
	leaves := make([][32]byte, n)
	for i := range leaves {
		leaves[i] = sha256.Sum256([]byte(fmt.Sprintf("leaf-%d", i)))
	}
	return leaves
}

func TestMerkleProofsVerifyForEveryLeaf(t *testing.T) {
	for n := 1; n <= 9; n++ {
		leaves := testLeaves(n)
		root, err := MerkleRoot(leaves)
		if err != nil {
			t.Fatalf("n=%d: MerkleRoot: %v", n, err)
		}
		for i := range leaves {
			proof, err := MerkleProof(leaves, i)
			if err != nil {
				t.Fatalf("n=%d i=%d: MerkleProof: %v", n, i, err)
			}
			if !VerifyProof(leaves[i], proof, root) {
				t.Errorf("n=%d i=%d: valid proof rejected", n, i)
			}
			if VerifyProof(sha256.Sum256([]byte("forged")), proof, root) {
				t.Errorf("n=%d i=%d: forged leaf accepted", n, i)
			}
		}
	}
}

func TestMerkleRootRejectsEmptyInputAndBadIndex(t *testing.T) {
	if _, err := MerkleRoot(nil); err == nil {
		t.Error("expected an error for zero leaves")
	}
	if _, err := MerkleProof(testLeaves(3), 3); err == nil {
		t.Error("expected an error for an out-of-range index")
	}
}

func TestHashPairIsOrderIndependent(t *testing.T) {
	leaves := testLeaves(2)
	if hashPair(leaves[0], leaves[1]) != hashPair(leaves[1], leaves[0]) {
		t.Error("hashPair must sort its inputs to match the contract")
	}
}

func TestLeafHashCoversImmutableFields(t *testing.T) {
	event := &domain.AuditEvent{
		ID: "e1", TenantID: "t1", Mode: domain.ModeLive, ActorType: "user", ActorID: "u1",
		Action: "wallet.create", ResourceType: "wallet", ResourceID: "w1",
		CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	base := LeafHash(event)

	tampered := *event
	tampered.Action = "wallet.delete"
	if LeafHash(&tampered) == base {
		t.Error("changing the action must change the leaf")
	}

	sameInstant := *event
	sameInstant.CreatedAt = event.CreatedAt.In(time.FixedZone("x", 3600))
	if LeafHash(&sameInstant) != base {
		t.Error("the leaf must not depend on the timestamp's zone")
	}
}

func TestCommitDayAttestsTheUTCDay(t *testing.T) {
	events := []*domain.AuditEvent{
		{ID: "a", CreatedAt: time.Date(2026, 3, 9, 1, 0, 0, 0, time.UTC)},
		{ID: "b", CreatedAt: time.Date(2026, 3, 9, 23, 0, 0, 0, time.UTC)},
	}
	source := &fakeEventSource{events: events}
	committer := &fakeCommitter{}
	daemon := NewAttestationDaemon(source, committer)

	if err := daemon.CommitDay(context.Background(), time.Date(2026, 3, 9, 15, 30, 0, 0, time.UTC)); err != nil {
		t.Fatalf("CommitDay: %v", err)
	}

	wantStart := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	if !source.from.Equal(wantStart) || !source.to.Equal(wantStart.Add(24*time.Hour)) {
		t.Errorf("queried [%s, %s), want the whole UTC day", source.from, source.to)
	}
	if want := uint64(wantStart.Unix() / 86400); committer.day != want {
		t.Errorf("day index = %d, want %d", committer.day, want)
	}
	wantRoot, _ := MerkleRoot([][32]byte{LeafHash(events[0]), LeafHash(events[1])})
	if committer.root != wantRoot {
		t.Error("committed root does not match the day's events")
	}
	var meta dayMetadata
	if err := json.Unmarshal([]byte(committer.metadata), &meta); err != nil || meta.EventCount != 2 {
		t.Errorf("metadata = %q (%v), want an event count of 2", committer.metadata, err)
	}
}

func TestCommitDaySkipsDaysWithoutEvents(t *testing.T) {
	committer := &fakeCommitter{}
	daemon := NewAttestationDaemon(&fakeEventSource{}, committer)

	if err := daemon.CommitDay(context.Background(), time.Now()); err != nil {
		t.Fatalf("CommitDay: %v", err)
	}
	if committer.calls != 0 {
		t.Errorf("committed %d times for an empty day, want 0", committer.calls)
	}
}

func TestCommitDayReportsSourceAndCommitFailures(t *testing.T) {
	events := []*domain.AuditEvent{{ID: "a"}}

	sourceErr := NewAttestationDaemon(&fakeEventSource{err: errors.New("db down")}, &fakeCommitter{})
	if err := sourceErr.CommitDay(context.Background(), time.Now()); err == nil {
		t.Error("expected the source error to surface")
	}

	commitErr := NewAttestationDaemon(&fakeEventSource{events: events}, &fakeCommitter{failures: 1})
	if err := commitErr.CommitDay(context.Background(), time.Now()); err == nil {
		t.Error("expected the commit error to surface")
	}
}

func TestCommitWithRetryRecoversFromTransientFailures(t *testing.T) {
	committer := &fakeCommitter{failures: 2}
	daemon := NewAttestationDaemon(&fakeEventSource{events: []*domain.AuditEvent{{ID: "a"}}}, committer)
	daemon.retryDelay = time.Millisecond

	daemon.commitWithRetry(context.Background(), time.Now())

	if committer.calls != 3 || committer.root == ([32]byte{}) {
		t.Errorf("calls = %d, want 3 with the last one committed", committer.calls)
	}
}

func TestCommitWithRetryGivesUp(t *testing.T) {
	committer := &fakeCommitter{failures: 100}
	daemon := NewAttestationDaemon(&fakeEventSource{events: []*domain.AuditEvent{{ID: "a"}}}, committer)
	daemon.retryDelay = time.Millisecond

	daemon.commitWithRetry(context.Background(), time.Now())

	if committer.calls != commitRetries {
		t.Errorf("calls = %d, want %d", committer.calls, commitRetries)
	}
}
