package anchor

import "testing"

func TestSEP31ValidationAndMemo(t *testing.T) {
	req := SEP31Request{Sender: Party{"123", "DEUTDEFF", "tax-1"}, Receiver: Party{"456", "BOFAUS3N", "tax-2"}, Asset: "USD", Amount: "10", Destination: "GDEST"}
	if err := Validate(req); err != nil {
		t.Fatal(err)
	}
	if len(ReceivingMemo(req.Destination)) != 27 {
		t.Fatal("memo length mismatch")
	}
}
