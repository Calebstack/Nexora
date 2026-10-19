package stellar

import "testing"

func TestTextMemoSanitizesAndBoundsUTF8(t *testing.T) { memo, err := TextMemo("hello\nworld"); if err != nil || memo.Text != "helloworld" { t.Fatalf("memo=%+v err=%v", memo, err) } }
func TestHashMemoRequires32Bytes(t *testing.T) { if _, err := HashMemo("hash", "aa"); err == nil { t.Fatal("expected invalid hash") } }
