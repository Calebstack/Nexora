package audit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/xdr"
)

const (
	secondsPerDay = 24 * 60 * 60

	// commitGrace lets audit writes that were in flight at midnight land before
	// the finished day is read.
	commitGrace = 30 * time.Second

	// commitRetries and commitRetryDelay bound how long the daemon keeps trying
	// to land a day's attestation after a transient failure.
	commitRetries    = 3
	commitRetryDelay = time.Minute
)

// EventSource returns every audit event created in [from, to) across tenants.
type EventSource interface {
	ListBetween(ctx context.Context, from, to time.Time) ([]*domain.AuditEvent, error)
}

// Committer publishes one daily Merkle root to the attestation contract and
// returns the transaction hash.
type Committer interface {
	Commit(ctx context.Context, day uint64, root [32]byte, metadata string) (string, error)
}

// LeafHash is the Merkle leaf for an audit event: the SHA-256 of its immutable
// fields in a fixed order. Auditors recompute it from an exported event and
// check inclusion with the proof against the on-chain root.
func LeafHash(e *domain.AuditEvent) [32]byte {
	fields := []string{
		e.ID,
		e.TenantID,
		string(e.Mode),
		e.ActorType,
		e.ActorID,
		e.Action,
		e.ResourceType,
		e.ResourceID,
		e.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	return sha256.Sum256([]byte(strings.Join(fields, "|")))
}

// hashPair hashes two siblings in sorted order, matching hash_pair in
// contracts/soroban/attestation/src/lib.rs.
func hashPair(a, b [32]byte) [32]byte {
	if bytes.Compare(a[:], b[:]) > 0 {
		a, b = b, a
	}
	return sha256.Sum256(append(a[:], b[:]...))
}

// MerkleRoot builds the root over leaves in order. An unpaired node is carried
// up a level unchanged.
func MerkleRoot(leaves [][32]byte) ([32]byte, error) {
	if len(leaves) == 0 {
		return [32]byte{}, fmt.Errorf("merkle root of zero leaves")
	}

	level := append([][32]byte(nil), leaves...)
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			next = append(next, hashPair(level[i], level[i+1]))
		}
		level = next
	}
	return level[0], nil
}

// MerkleProof returns the sibling hashes from leaves[index] up to the root, in
// the order the contract's verify_inclusion consumes them.
func MerkleProof(leaves [][32]byte, index int) ([][32]byte, error) {
	if index < 0 || index >= len(leaves) {
		return nil, fmt.Errorf("leaf index %d out of range for %d leaves", index, len(leaves))
	}

	var proof [][32]byte
	level := append([][32]byte(nil), leaves...)
	for len(level) > 1 {
		sibling := index ^ 1
		if sibling < len(level) {
			proof = append(proof, level[sibling])
		}

		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			next = append(next, hashPair(level[i], level[i+1]))
		}
		level = next
		index /= 2
	}
	return proof, nil
}

// VerifyProof checks that leaf is included in root given its proof.
func VerifyProof(leaf [32]byte, proof [][32]byte, root [32]byte) bool {
	computed := leaf
	for _, sibling := range proof {
		computed = hashPair(computed, sibling)
	}
	return computed == root
}

// dayMetadata is the metadata stored next to each on-chain root.
type dayMetadata struct {
	EventCount  int    `json:"event_count"`
	WindowStart string `json:"window_start"`
	WindowEnd   string `json:"window_end"`
}

// AttestationDaemon commits a Merkle root of each UTC day's audit events to the
// attestation contract shortly after midnight UTC.
type AttestationDaemon struct {
	source     EventSource
	committer  Committer
	now        func() time.Time
	retryDelay time.Duration
}

func NewAttestationDaemon(source EventSource, committer Committer) *AttestationDaemon {
	return &AttestationDaemon{
		source:     source,
		committer:  committer,
		now:        time.Now,
		retryDelay: commitRetryDelay,
	}
}

// CommitDay attests the UTC day containing day. A day without audit events has
// nothing to attest and is skipped.
func (d *AttestationDaemon) CommitDay(ctx context.Context, day time.Time) error {
	start := day.UTC().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)

	events, err := d.source.ListBetween(ctx, start, end)
	if err != nil {
		return fmt.Errorf("list audit events for %s: %w", start.Format("2006-01-02"), err)
	}
	if len(events) == 0 {
		log.Info().Str("day", start.Format("2006-01-02")).Msg("no audit events to attest")
		return nil
	}

	leaves := make([][32]byte, 0, len(events))
	for _, e := range events {
		leaves = append(leaves, LeafHash(e))
	}
	root, err := MerkleRoot(leaves)
	if err != nil {
		return err
	}

	metadata, err := json.Marshal(dayMetadata{
		EventCount:  len(events),
		WindowStart: start.Format(time.RFC3339),
		WindowEnd:   end.Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("encode attestation metadata: %w", err)
	}

	txHash, err := d.committer.Commit(ctx, uint64(start.Unix()/secondsPerDay), root, string(metadata))
	if err != nil {
		return fmt.Errorf("commit attestation for %s: %w", start.Format("2006-01-02"), err)
	}

	log.Info().
		Str("day", start.Format("2006-01-02")).
		Int("events", len(events)).
		Str("tx_hash", txHash).
		Msg("audit attestation committed")
	return nil
}

// Run sleeps until just after each midnight UTC and then attests the day that just ended,
// retrying a few times on failure. It returns when ctx is cancelled.
func (d *AttestationDaemon) Run(ctx context.Context) {
	for {
		next := d.now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
		timer := time.NewTimer(next.Add(commitGrace).Sub(d.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		previousDay := next.Add(-time.Nanosecond)
		d.commitWithRetry(ctx, previousDay)
	}
}

func (d *AttestationDaemon) commitWithRetry(ctx context.Context, day time.Time) {
	for attempt := 1; attempt <= commitRetries; attempt++ {
		err := d.CommitDay(ctx, day)
		if err == nil {
			return
		}
		log.Error().Err(err).Int("attempt", attempt).Msg("audit attestation failed")

		if attempt == commitRetries {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.retryDelay):
		}
	}
}

// SorobanCommitter publishes roots to the attestation contract with a
// maintainer key registered via add_maintainer.
type SorobanCommitter struct {
	client          stellar.SorobanClient
	signer          stellar.Signer
	contractID      string
	maintainer      string
	encryptedSecret string
}

func NewSorobanCommitter(client stellar.SorobanClient, signer stellar.Signer, contractID, maintainer, encryptedSecret string) *SorobanCommitter {
	return &SorobanCommitter{
		client:          client,
		signer:          signer,
		contractID:      contractID,
		maintainer:      maintainer,
		encryptedSecret: encryptedSecret,
	}
}

func (c *SorobanCommitter) Commit(ctx context.Context, day uint64, root [32]byte, metadata string) (string, error) {
	maintainer, err := stellar.AddressScVal(c.maintainer)
	if err != nil {
		return "", err
	}

	op, err := stellar.InvokeContractOp(c.contractID, "publish_attestation", c.maintainer, []xdr.ScVal{
		maintainer,
		stellar.U64ScVal(day),
		stellar.BytesScVal(root[:]),
		stellar.StringScVal(metadata),
	})
	if err != nil {
		return "", err
	}

	tx, err := c.client.PrepareInvocation(ctx, c.maintainer, op)
	if err != nil {
		return "", fmt.Errorf("prepare publish_attestation: %w", err)
	}
	signed, err := c.signer.Sign(tx, c.encryptedSecret)
	if err != nil {
		return "", fmt.Errorf("sign publish_attestation: %w", err)
	}
	hash, err := c.client.SubmitTransaction(ctx, signed)
	if err != nil {
		return "", fmt.Errorf("submit publish_attestation: %w", err)
	}
	return hash, nil
}
