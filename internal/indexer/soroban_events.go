package indexer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/xdr"
)

// ErrLedgerGap means the event source returned a ledger that does not follow
// the last indexed one. Nothing is stored; the next sync retries from the same
// sequence.
var ErrLedgerGap = errors.New("soroban event ledger sequence gap")

// ErrReorgTooDeep means no indexed ledger matched the canonical chain within
// the configured rollback window.
var ErrReorgTooDeep = errors.New("soroban event re-org deeper than rollback window")

// RawContractEvent is a contract event as returned by Soroban RPC, with the
// topics and value still base64 XDR ScVals.
type RawContractEvent struct {
	ID         string
	ContractID string
	Type       string
	TxHash     string
	TopicsXDR  []string
	ValueXDR   string
}

// LedgerEvents holds every contract event emitted in one ledger.
type LedgerEvents struct {
	Sequence uint32
	Hash     string
	PrevHash string
	ClosedAt time.Time
	Events   []RawContractEvent
}

// SorobanEventSource reads contract events ledger by ledger.
type SorobanEventSource interface {
	LatestLedger(ctx context.Context) (uint32, error)
	LedgerEvents(ctx context.Context, sequence uint32) (*LedgerEvents, error)
}

// ContractEventStore persists decoded events together with the ledgers that
// produced them.
type ContractEventStore interface {
	// SaveLedger stores the ledger and its events atomically and returns only
	// the events that were not already stored.
	SaveLedger(ctx context.Context, ledger domain.IndexedLedger, events []*domain.ContractEvent) ([]*domain.ContractEvent, error)
	// GetLedger and LastLedger return nil when the ledger is not indexed.
	GetLedger(ctx context.Context, sequence uint32) (*domain.IndexedLedger, error)
	LastLedger(ctx context.Context) (*domain.IndexedLedger, error)
	// DeleteFromLedger drops every ledger and event at or above sequence.
	DeleteFromLedger(ctx context.Context, sequence uint32) error
}

// EventDispatcher is satisfied by webhook.Dispatcher.
type EventDispatcher interface {
	Dispatch(ctx context.Context, eventType string, payload interface{}) error
}

// SorobanEventConfig tunes the contract event indexer.
type SorobanEventConfig struct {
	// StartLedger is where indexing begins when nothing is indexed yet. Zero
	// starts at the current latest ledger.
	StartLedger uint32
	// MaxReorgDepth bounds how many indexed ledgers a re-org may roll back.
	MaxReorgDepth uint32
}

func DefaultSorobanEventConfig() SorobanEventConfig {
	return SorobanEventConfig{MaxReorgDepth: 100}
}

// SorobanEventIndexer ingests Soroban contract events into the store and fires
// a contract.event.emitted webhook for each newly stored event.
type SorobanEventIndexer struct {
	source     SorobanEventSource
	store      ContractEventStore
	dispatcher EventDispatcher
	config     SorobanEventConfig
}

func NewSorobanEventIndexer(source SorobanEventSource, store ContractEventStore, dispatcher EventDispatcher, config SorobanEventConfig) *SorobanEventIndexer {
	if config.MaxReorgDepth == 0 {
		config.MaxReorgDepth = DefaultSorobanEventConfig().MaxReorgDepth
	}
	return &SorobanEventIndexer{source: source, store: store, dispatcher: dispatcher, config: config}
}

// Sync indexes every ledger between the last indexed one and the latest ledger.
// A parent-hash mismatch rolls the index back to the fork point before it
// continues, so events from orphaned ledgers never remain.
func (s *SorobanEventIndexer) Sync(ctx context.Context) error {
	latest, err := s.source.LatestLedger(ctx)
	if err != nil {
		return fmt.Errorf("get latest ledger: %w", err)
	}

	last, err := s.store.LastLedger(ctx)
	if err != nil {
		return fmt.Errorf("get last indexed ledger: %w", err)
	}

	var next uint32
	switch {
	case last != nil:
		next = last.Sequence + 1
	case s.config.StartLedger > 0:
		next = s.config.StartLedger
	default:
		next = latest
	}

	for next <= latest {
		if err := ctx.Err(); err != nil {
			return err
		}

		ledger, err := s.source.LedgerEvents(ctx, next)
		if err != nil {
			return fmt.Errorf("fetch events for ledger %d: %w", next, err)
		}
		if ledger.Sequence != next {
			return fmt.Errorf("%w: expected ledger %d, got %d", ErrLedgerGap, next, ledger.Sequence)
		}

		if last != nil && ledger.PrevHash != last.Hash {
			log.Warn().Uint32("ledger", next).Msg("indexer: soroban re-org detected, rolling back")
			last, err = s.rollback(ctx, last)
			if err != nil {
				return err
			}
			next = last.Sequence + 1
			continue
		}

		if err := s.indexLedger(ctx, ledger); err != nil {
			return err
		}
		last = &domain.IndexedLedger{Sequence: ledger.Sequence, Hash: ledger.Hash}
		next++
	}
	return nil
}

// rollback walks back from the last indexed ledger until a stored hash matches
// the canonical chain, deletes everything after that fork point, and returns
// the fork point.
func (s *SorobanEventIndexer) rollback(ctx context.Context, last *domain.IndexedLedger) (*domain.IndexedLedger, error) {
	for depth := uint32(0); depth < s.config.MaxReorgDepth && last.Sequence >= depth; depth++ {
		seq := last.Sequence - depth
		stored, err := s.store.GetLedger(ctx, seq)
		if err != nil {
			return nil, fmt.Errorf("get indexed ledger %d: %w", seq, err)
		}
		if stored == nil {
			continue
		}
		canonical, err := s.source.LedgerEvents(ctx, seq)
		if err != nil {
			return nil, fmt.Errorf("fetch canonical ledger %d: %w", seq, err)
		}
		if canonical.Hash == stored.Hash {
			if err := s.store.DeleteFromLedger(ctx, seq+1); err != nil {
				return nil, fmt.Errorf("roll back from ledger %d: %w", seq+1, err)
			}
			return stored, nil
		}
	}
	return nil, fmt.Errorf("%w: last indexed ledger %d", ErrReorgTooDeep, last.Sequence)
}

func (s *SorobanEventIndexer) indexLedger(ctx context.Context, ledger *LedgerEvents) error {
	events := make([]*domain.ContractEvent, 0, len(ledger.Events))
	for _, raw := range ledger.Events {
		event, err := DecodeContractEvent(raw, ledger.Sequence, ledger.ClosedAt)
		if err != nil {
			log.Error().Err(err).Str("event_id", raw.ID).Uint32("ledger", ledger.Sequence).
				Msg("indexer: skipping undecodable contract event")
			continue
		}
		events = append(events, event)
	}

	inserted, err := s.store.SaveLedger(ctx, domain.IndexedLedger{Sequence: ledger.Sequence, Hash: ledger.Hash}, events)
	if err != nil {
		return fmt.Errorf("save ledger %d: %w", ledger.Sequence, err)
	}

	if s.dispatcher == nil {
		return nil
	}
	for _, event := range inserted {
		if err := s.dispatcher.Dispatch(ctx, domain.EventContractEventEmitted, event); err != nil {
			log.Error().Err(err).Str("event_id", event.ID).Msg("indexer: dispatch contract event webhook failed")
		}
	}
	return nil
}

// DecodeContractEvent decodes the XDR topics and value of a raw event into JSON.
func DecodeContractEvent(raw RawContractEvent, ledger uint32, closedAt time.Time) (*domain.ContractEvent, error) {
	topics := make([]interface{}, 0, len(raw.TopicsXDR))
	for i, encoded := range raw.TopicsXDR {
		topic, err := decodeScValJSON(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode topic %d: %w", i, err)
		}
		topics = append(topics, topic)
	}

	data, err := decodeScValJSON(raw.ValueXDR)
	if err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}

	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return nil, fmt.Errorf("encode topics: %w", err)
	}
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}

	return &domain.ContractEvent{
		ID:         raw.ID,
		ContractID: raw.ContractID,
		EventType:  raw.Type,
		Topic:      firstTopic(topics),
		Topics:     topicsJSON,
		Data:       dataJSON,
		Ledger:     ledger,
		TxHash:     raw.TxHash,
		CreatedAt:  closedAt,
	}, nil
}

// firstTopic is the indexed topic column: by convention the first topic names
// the event (for example "transfer").
func firstTopic(topics []interface{}) string {
	if len(topics) == 0 {
		return ""
	}
	if s, ok := topics[0].(string); ok {
		return s
	}
	encoded, err := json.Marshal(topics[0])
	if err != nil {
		return ""
	}
	return string(encoded)
}

func decodeScValJSON(encoded string) (interface{}, error) {
	if encoded == "" {
		return nil, nil
	}
	var v xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(encoded, &v); err != nil {
		return nil, fmt.Errorf("decode scval: %w", err)
	}
	return scValToJSON(v)
}

// scValToJSON converts a contract value into JSON-friendly Go values. Integers
// wider than 32 bits become decimal strings so they survive JSON consumers that
// parse numbers as float64.
func scValToJSON(v xdr.ScVal) (interface{}, error) {
	switch v.Type {
	case xdr.ScValTypeScvVoid:
		return nil, nil
	case xdr.ScValTypeScvBool:
		b, _ := v.GetB()
		return b, nil
	case xdr.ScValTypeScvU32:
		u, _ := v.GetU32()
		return uint32(u), nil
	case xdr.ScValTypeScvI32:
		i, _ := v.GetI32()
		return int32(i), nil
	case xdr.ScValTypeScvU64:
		u, _ := v.GetU64()
		return strconv.FormatUint(uint64(u), 10), nil
	case xdr.ScValTypeScvI64:
		i, _ := v.GetI64()
		return strconv.FormatInt(int64(i), 10), nil
	case xdr.ScValTypeScvTimepoint:
		t, _ := v.GetTimepoint()
		return strconv.FormatUint(uint64(t), 10), nil
	case xdr.ScValTypeScvDuration:
		d, _ := v.GetDuration()
		return strconv.FormatUint(uint64(d), 10), nil
	case xdr.ScValTypeScvU128:
		parts, _ := v.GetU128()
		n := new(big.Int).SetUint64(uint64(parts.Hi))
		n.Lsh(n, 64)
		n.Or(n, new(big.Int).SetUint64(uint64(parts.Lo)))
		return n.String(), nil
	case xdr.ScValTypeScvI128:
		n, err := stellar.DecodeI128(v)
		if err != nil {
			return nil, err
		}
		return n.String(), nil
	case xdr.ScValTypeScvBytes:
		b, _ := v.GetBytes()
		return hex.EncodeToString(b), nil
	case xdr.ScValTypeScvString:
		s, _ := v.GetStr()
		return string(s), nil
	case xdr.ScValTypeScvSymbol:
		s, _ := v.GetSym()
		return string(s), nil
	case xdr.ScValTypeScvAddress:
		return stellar.DecodeAddress(v)
	case xdr.ScValTypeScvVec:
		vec, ok := v.GetVec()
		if !ok || vec == nil {
			return []interface{}{}, nil
		}
		items := make([]interface{}, 0, len(*vec))
		for _, item := range *vec {
			decoded, err := scValToJSON(item)
			if err != nil {
				return nil, err
			}
			items = append(items, decoded)
		}
		return items, nil
	case xdr.ScValTypeScvMap:
		m, ok := v.GetMap()
		if !ok || m == nil {
			return map[string]interface{}{}, nil
		}
		fields := make(map[string]interface{}, len(*m))
		for _, entry := range *m {
			key, err := scValToJSON(entry.Key)
			if err != nil {
				return nil, err
			}
			val, err := scValToJSON(entry.Val)
			if err != nil {
				return nil, err
			}
			if name, ok := key.(string); ok {
				fields[name] = val
				continue
			}
			keyJSON, err := json.Marshal(key)
			if err != nil {
				return nil, fmt.Errorf("encode map key: %w", err)
			}
			fields[string(keyJSON)] = val
		}
		return fields, nil
	default:
		// Types without a natural JSON form (errors, 256-bit integers, ledger
		// keys, contract instances) keep their raw XDR so no data is lost.
		raw, err := xdr.MarshalBase64(v)
		if err != nil {
			return nil, fmt.Errorf("encode %s as xdr: %w", v.Type, err)
		}
		return map[string]interface{}{"type": v.Type.String(), "xdr": raw}, nil
	}
}
