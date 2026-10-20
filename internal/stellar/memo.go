package stellar

import (
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

type Memo struct { Type string; Text string; ID uint64; Bytes []byte }
func TextMemo(value string) (Memo, error) { value = strings.Map(func(r rune) rune { if r < 0x20 || r == 0x7f { return -1 }; return r }, value); if !utf8.ValidString(value) || len([]byte(value)) > 28 { return Memo{}, fmt.Errorf("text memo must be valid UTF-8 and at most 28 bytes") }; return Memo{Type: "text", Text: value}, nil }
func IDMemo(value uint64) Memo { return Memo{Type: "id", ID: value} }
func HashMemo(kind, value string) (Memo, error) { if kind != "hash" && kind != "return" { return Memo{}, fmt.Errorf("unsupported memo type") }; decoded, err := hex.DecodeString(strings.TrimPrefix(value, "0x")); if err != nil || len(decoded) != 32 { return Memo{}, fmt.Errorf("%s memo must be exactly 32 bytes of hex", kind) }; return Memo{Type: kind, Bytes: decoded}, nil }
type MemoPolicy map[string]bool
func (p MemoPolicy) Require(destination string, memo Memo) error { if !p[destination] { return nil }; if memo.Type == "" { return fmt.Errorf("memo required for destination %s", destination) }; return nil }
