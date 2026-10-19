package anchor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

type Party struct{ AccountNumber, SWIFTBIC, TaxID string }
type SEP31Request struct {
	Sender, Receiver           Party
	Asset, Amount, Destination string
}
type Validator func(SEP31Request) error

func Validate(req SEP31Request) error {
	if req.Asset == "" || req.Amount == "" || req.Destination == "" {
		return fmt.Errorf("asset, amount, and destination are required")
	}
	for name, party := range map[string]Party{"sender": req.Sender, "receiver": req.Receiver} {
		if party.AccountNumber == "" || party.SWIFTBIC == "" || party.TaxID == "" {
			return fmt.Errorf("%s KYC requires account number, SWIFT/BIC, and tax ID", name)
		}
		if len(party.SWIFTBIC) < 8 || len(party.SWIFTBIC) > 11 {
			return fmt.Errorf("invalid %s SWIFT/BIC", name)
		}
	}
	return nil
}
func ReceivingMemo(destination string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(destination)))
	return "NEX" + hex.EncodeToString(sum[:])[:24]
}
