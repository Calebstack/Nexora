package stellar

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stellar/go/support/render/problem"
	"github.com/stellar/go/txnbuild"
)

// defaultHorizonTimeout bounds every Horizon HTTP round-trip. This Horizon SDK
// release has no per-call context support for non-streaming endpoints, so the
// HTTP client timeout is what enforces the deadline; the *WithContext helpers
// additionally derive a bounded context so work stops when the caller's
// context is already done.
const defaultHorizonTimeout = 10 * time.Second

// Client is the interface Nexora uses to interact with Stellar/Horizon.
type Client interface {
	LoadAccount(accountID string) (horizon.Account, error)
	SubmitTransaction(tx *txnbuild.Transaction) (horizon.Transaction, error)
	// FindPathsStrict returns strict-receive path-payment candidates for a
	// payment from sourceAccount to destAccount.
	FindPathsStrict(sourceAccount, destAccount, destAsset, destIssuer, destAmount string) ([]horizon.Path, error)
	TransactionDetail(hash string) (horizon.Transaction, error)
	OperationsForTransaction(hash string) ([]operations.Operation, error)
	// Payments returns a page of payment operations for an account, starting
	// strictly after cursor (empty cursor starts from the account's first payment).
	Payments(accountID, cursor string, limit uint) ([]operations.Operation, error)
	// StreamPayments streams payment operations for an account in real time,
	// starting strictly after cursor (empty cursor streams only new payments).
	// It blocks until ctx is canceled or the stream errors.
	StreamPayments(ctx context.Context, accountID, cursor string, handler func(operations.Operation) error) error
	// Offers returns the open offers for an account, up to limit (capped by
	// Horizon at 200 per page).
	Offers(accountID string, limit uint) ([]horizon.Offer, error)
}

type horizonClient struct {
	inner   *horizonclient.Client
	network string
	timeout time.Duration
}

// NewClient builds a Horizon client whose HTTP requests are bounded by timeout.
// A non-positive timeout falls back to defaultHorizonTimeout.
func NewClient(horizonURL, network string, timeout time.Duration) Client {
	if timeout <= 0 {
		timeout = defaultHorizonTimeout
	}
	return &horizonClient{
		inner: &horizonclient.Client{
			HorizonURL: horizonURL,
			HTTP:       &http.Client{Timeout: timeout},
		},
		network: network,
		timeout: timeout,
	}
}

func (c *horizonClient) LoadAccount(accountID string) (horizon.Account, error) {
	acct, err := c.inner.AccountDetail(horizonclient.AccountRequest{AccountID: accountID})
	if err != nil {
		return horizon.Account{}, fmt.Errorf("load account %s: %w", accountID, err)
	}
	return acct, nil
}

func (c *horizonClient) SubmitTransaction(tx *txnbuild.Transaction) (horizon.Transaction, error) {
	resp, err := c.inner.SubmitTransaction(tx)
	if err != nil {
		return horizon.Transaction{}, fmt.Errorf("submit transaction: %w", err)
	}
	return resp, nil
}

func (c *horizonClient) TransactionDetail(hash string) (horizon.Transaction, error) {
	tx, err := c.inner.TransactionDetail(hash)
	if err != nil {
		return horizon.Transaction{}, fmt.Errorf("transaction detail: %w", err)
	}
	return tx, nil
}

func (c *horizonClient) OperationsForTransaction(hash string) ([]operations.Operation, error) {
	page, err := c.inner.Operations(horizonclient.OperationRequest{ForTransaction: hash})
	if err != nil {
		return nil, fmt.Errorf("operations for transaction: %w", err)
	}
	return page.Embedded.Records, nil
}

func (c *horizonClient) Payments(accountID, cursor string, limit uint) ([]operations.Operation, error) {
	page, err := c.inner.Payments(horizonclient.OperationRequest{
		ForAccount: accountID,
		Cursor:     cursor,
		Limit:      limit,
		Order:      horizonclient.OrderAsc,
		Join:       "transactions",
	})
	if err != nil {
		return nil, fmt.Errorf("payments for account %s: %w", accountID, err)
	}
	return page.Embedded.Records, nil
}

func (c *horizonClient) StreamPayments(ctx context.Context, accountID, cursor string, handler func(operations.Operation) error) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var handlerErr error
	err := c.inner.StreamPayments(streamCtx, horizonclient.OperationRequest{
		ForAccount: accountID,
		Cursor:     cursor,
		Join:       "transactions",
	}, func(op operations.Operation) {
		if handlerErr != nil {
			return
		}
		if err := handler(op); err != nil {
			handlerErr = err
			cancel() // stop the stream promptly; don't process further events
		}
	})
	if handlerErr != nil {
		return handlerErr
	}
	if err != nil {
		return fmt.Errorf("stream payments for account %s: %w", accountID, err)
	}
	return nil
}

func (c *horizonClient) Offers(accountID string, limit uint) ([]horizon.Offer, error) {
	page, err := c.inner.Offers(horizonclient.OfferRequest{
		ForAccount: accountID,
		Limit:      limit,
	})
	if err != nil {
		return nil, fmt.Errorf("offers for account %s: %w", accountID, err)
	}
	return page.Embedded.Records, nil
}

// FindPathsStrict runs a strict-receive path search. sourceAccount is the
// account paying (the path's source_account), destAccount is the account
// receiving destAsset. Both must be populated: the previous implementation
// wrote the paying account into the destination slot.
func (c *horizonClient) FindPathsStrict(sourceAccount, destAccount, destAsset, destIssuer, destAmount string) ([]horizon.Path, error) {
	req := horizonclient.PathsRequest{
		SourceAccount:          sourceAccount,
		DestinationAccount:     destAccount,
		DestinationAssetType:   horizonclient.AssetType4,
		DestinationAssetCode:   destAsset,
		DestinationAssetIssuer: destIssuer,
		DestinationAmount:      destAmount,
	}
	paths, err := c.inner.Paths(req)
	if err != nil {
		return nil, fmt.Errorf("find paths: %w", err)
	}
	return paths.Embedded.Records, nil
}

// txBadSeq is the Horizon/Stellar Core result code for a transaction whose
// sequence number does not match its source account. Core rejects such a
// transaction at validation: it is not applied and charges no fee.
const txBadSeq = "tx_bad_seq"

// IsBadSequence reports whether a submission was rejected with tx_bad_seq,
// either on the transaction itself or on the inner transaction of a fee bump.
func IsBadSequence(err error) bool {
	var herr *horizonclient.Error
	if !errors.As(err, &herr) || herr == nil {
		return false
	}
	codes, err := herr.ResultCodes()
	if err != nil || codes == nil {
		return false
	}
	return codes.TransactionCode == txBadSeq || codes.InnerTransactionCode == txBadSeq
}

// HorizonEvent is one Server-Sent Event from a Horizon streaming endpoint.
// Horizon sets ID to the record's paging token, so it is also the cursor to
// resume from.
type HorizonEvent struct {
	ID    string
	Event string
	Data  []byte
}

var (
	// ErrStreamClosed means Horizon ended the stream normally with its
	// "close" event (sent when the stream's page limit is reached); the
	// client is expected to reconnect from the last cursor.
	ErrStreamClosed = errors.New("horizon closed the stream")
	// ErrStreamIdle means no bytes arrived within the idle timeout. Horizon
	// sends no keepalives, so this bounds how long a silently dead TCP
	// connection can go unnoticed.
	ErrStreamIdle = errors.New("horizon stream idle timeout")
)

// maxSSELine bounds one SSE line; Horizon transaction records carry XDR and
// can be large.
const maxSSELine = 4 << 20

// HorizonStreamer opens Horizon SSE connections. Streams are long-lived, so
// its HTTP client has no overall timeout: TCP keepalives and the idle timeout
// detect dead connections instead.
type HorizonStreamer struct {
	baseURL     string
	client      *http.Client
	idleTimeout time.Duration
}

// NewHorizonStreamer builds a streamer for horizonURL. A non-positive
// idleTimeout disables idle detection.
func NewHorizonStreamer(horizonURL string, idleTimeout time.Duration) *HorizonStreamer {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: defaultHorizonTimeout, KeepAlive: 15 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = defaultHorizonTimeout
	return NewHorizonStreamerWithClient(horizonURL, &http.Client{Transport: transport}, idleTimeout)
}

// NewHorizonStreamerWithClient uses client, which must not set Timeout.
func NewHorizonStreamerWithClient(horizonURL string, client *http.Client, idleTimeout time.Duration) *HorizonStreamer {
	return &HorizonStreamer{baseURL: strings.TrimRight(horizonURL, "/"), client: client, idleTimeout: idleTimeout}
}

// Stream opens one SSE connection to path (for example
// "/accounts/G.../payments") and blocks until it ends. It resumes strictly
// after lastEventID, sent as both the cursor and the Last-Event-ID header
// (which Horizon prefers); an empty lastEventID streams from the query's
// cursor, or "now". onActivity runs for every line read, including comments
// and the "open" preamble; onEvent runs for every data event.
//
// It returns ErrStreamClosed on Horizon's "close" event, ErrStreamIdle after
// idleTimeout of silence, a *horizonclient.Error for a non-200 response (so
// HTTPStatus and RetryAfter apply), an error for Horizon's "error" event, the
// error returned by onEvent, or the read error. O(1) memory per event beyond
// its data.
func (s *HorizonStreamer) Stream(ctx context.Context, path string, query url.Values, lastEventID string, onActivity func(), onEvent func(HorizonEvent) error) error {
	q := url.Values{}
	for k, v := range query {
		q[k] = v
	}
	if lastEventID != "" {
		q.Set("cursor", lastEventID)
	} else if q.Get("cursor") == "" {
		q.Set("cursor", "now")
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, s.baseURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return fmt.Errorf("build stream request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	var (
		idle  *time.Timer
		idled atomic.Bool
	)
	if s.idleTimeout > 0 {
		idle = time.AfterFunc(s.idleTimeout, func() { idled.Store(true); cancel() })
		defer idle.Stop()
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return streamErr(ctx, &idled, fmt.Errorf("open stream %s: %w", path, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		herr := &horizonclient.Error{Response: resp}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = json.Unmarshal(body, &herr.Problem)
		if herr.Problem.Status == 0 {
			herr.Problem = problem.P{Status: resp.StatusCode, Title: resp.Status}
		}
		return herr
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), maxSSELine)
	var (
		data      bytes.Buffer
		event, id string
	)
	for scanner.Scan() {
		if idle != nil {
			if idled.Load() {
				return ErrStreamIdle
			}
			idle.Reset(s.idleTimeout)
		}
		if onActivity != nil {
			onActivity()
		}
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			// A blank line dispatches the buffered event.
			if data.Len() > 0 {
				ev := HorizonEvent{ID: id, Event: event, Data: bytes.TrimSuffix(data.Bytes(), []byte("\n"))}
				switch ev.Event {
				case "open": // Horizon's "hello" preamble
				case "close":
					return ErrStreamClosed
				case "error":
					return fmt.Errorf("horizon stream error: %s", ev.Data)
				default:
					if err := onEvent(ev); err != nil {
						return err
					}
				}
			}
			data.Reset()
			event = ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
		case "event":
			event = value
		case "id":
			if !strings.ContainsRune(value, 0) {
				id = value
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return streamErr(ctx, &idled, fmt.Errorf("read stream %s: %w", path, err))
	}
	return streamErr(ctx, &idled, fmt.Errorf("read stream %s: %w", path, io.ErrUnexpectedEOF))
}

// streamErr reports ErrStreamIdle when the idle timer cancelled the
// connection, and the caller's context error when it was cancelled.
func streamErr(ctx context.Context, idled *atomic.Bool, err error) error {
	if idled.Load() {
		return ErrStreamIdle
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
