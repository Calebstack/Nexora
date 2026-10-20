package stellar

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
)

var (
	// ErrOfflineSignaturePending indicates that an unsigned transaction XDR has been
	// cached for asynchronous air-gapped signing and cannot be broadcast immediately.
	ErrOfflineSignaturePending = errors.New("offline signature pending: unsigned XDR cached for air-gapped signing")

	// ErrNoAuthorizedSigners indicates that no authorized signing public keys were configured.
	ErrNoAuthorizedSigners = errors.New("at least one authorized signing public key is required")

	// ErrUnauthorizedSignature indicates that an envelope carries a signature from a key
	// not in the authorized signers list, or fails cryptographic verification.
	ErrUnauthorizedSignature = errors.New("signature verification failed against authorized signing keys")

	// ErrInsufficientSignatures indicates that fewer valid signatures were provided than required.
	ErrInsufficientSignatures = errors.New("insufficient valid signatures for transaction broadcast")
)

// Signer signs a Stellar transaction. The abstraction boundary that separates
// key management from transaction building — replace EnvSigner with an HSM,
// KMS, or air-gapped OfflineSigner implementation without touching the settlement engine.
type Signer interface {
	Sign(tx *txnbuild.Transaction, encryptedSecret string) (*txnbuild.Transaction, error)
}

// EnvSigner decrypts the wallet secret from AES-256-GCM storage and signs in-process.
type EnvSigner struct {
	masterKey         []byte
	networkPassphrase string
}

func NewEnvSigner(masterKey []byte, stellarNetwork string) *EnvSigner {
	passphrase := ResolveNetworkPassphrase(stellarNetwork)
	return &EnvSigner{masterKey: masterKey, networkPassphrase: passphrase}
}

// ResolveNetworkPassphrase maps a network identifier ("mainnet", "public", "testnet", or a raw passphrase)
// to the canonical Stellar network passphrase.
func ResolveNetworkPassphrase(stellarNetwork string) string {
	switch strings.ToLower(strings.TrimSpace(stellarNetwork)) {
	case "mainnet", "public":
		return network.PublicNetworkPassphrase
	case "", "testnet":
		return network.TestNetworkPassphrase
	default:
		return stellarNetwork
	}
}

func (s *EnvSigner) Sign(tx *txnbuild.Transaction, encryptedSecret string) (*txnbuild.Transaction, error) {
	secretBytes, err := crypto.Decrypt([]byte(encryptedSecret), s.masterKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt secret: %w", err)
	}

	kp, err := keypair.ParseFull(string(secretBytes))
	if err != nil {
		return nil, fmt.Errorf("parse keypair: %w", err)
	}

	signed, err := tx.Sign(s.networkPassphrase, kp)
	if err != nil {
		return nil, fmt.Errorf("sign transaction: %w", err)
	}

	return signed, nil
}

// VerifyTransactionSignatures cryptographically verifies every decorated signature on tx
// against the set of authorized Stellar public keys (G... addresses).
// It returns the list of unique authorized public keys that validly signed tx, or an error
// if any attached signature is invalid/unauthorized or if fewer than minRequired valid
// signatures are present.
func VerifyTransactionSignatures(tx *txnbuild.Transaction, networkPassphrase string, authorizedPublicKeys []string, minRequired int) ([]string, error) {
	if tx == nil {
		return nil, errors.New("transaction is nil")
	}
	if minRequired <= 0 {
		minRequired = 1
	}
	if len(authorizedPublicKeys) == 0 {
		return nil, ErrNoAuthorizedSigners
	}

	authorizedKPs := make([]*keypair.FromAddress, 0, len(authorizedPublicKeys))
	seenAuth := make(map[string]struct{}, len(authorizedPublicKeys))
	for _, rawKey := range authorizedPublicKeys {
		addr := strings.TrimSpace(rawKey)
		if addr == "" {
			continue
		}
		if _, dup := seenAuth[addr]; dup {
			continue
		}
		kp, err := keypair.ParseAddress(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid authorized public key %q: %w", addr, err)
		}
		seenAuth[addr] = struct{}{}
		authorizedKPs = append(authorizedKPs, kp)
	}
	if len(authorizedKPs) == 0 {
		return nil, ErrNoAuthorizedSigners
	}

	txHash, err := tx.Hash(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("compute transaction hash for verification: %w", err)
	}

	sigs := tx.Signatures()
	if len(sigs) == 0 {
		return nil, fmt.Errorf("%w: transaction has 0 signatures (need %d)", ErrInsufficientSignatures, minRequired)
	}

	verifiedSigners := make([]string, 0, len(sigs))
	verifiedSet := make(map[string]struct{}, len(sigs))

	for _, decoratedSig := range sigs {
		matched := false
		for _, kp := range authorizedKPs {
			if kp.Hint() != decoratedSig.Hint {
				continue
			}
			if err := kp.Verify(txHash[:], decoratedSig.Signature); err == nil {
				matched = true
				if _, exists := verifiedSet[kp.Address()]; !exists {
					verifiedSet[kp.Address()] = struct{}{}
					verifiedSigners = append(verifiedSigners, kp.Address())
				}
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("%w: signature with hint %x does not match any authorized signing key", ErrUnauthorizedSignature, decoratedSig.Hint)
		}
	}

	if len(verifiedSigners) < minRequired {
		return nil, fmt.Errorf("%w: got %d valid authorized signatures, require at least %d", ErrInsufficientSignatures, len(verifiedSigners), minRequired)
	}

	return verifiedSigners, nil
}

