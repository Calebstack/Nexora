package compliance_test

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/compliance"
)

type mockAuditRecorder struct {
	actions []string
	events  []map[string]interface{}
}

func (m *mockAuditRecorder) RecordCustomerAudit(ctx context.Context, action, customerID, tenantID string, metadata map[string]interface{}) error {
	m.actions = append(m.actions, action)
	m.events = append(m.events, metadata)
	return nil
}

func TestCustomerService_LifecycleAndEncryption(t *testing.T) {
	ctx := context.Background()
	masterKey := make([]byte, 32)
	_, _ = rand.Read(masterKey)

	audit := &mockAuditRecorder{}
	repo := compliance.NewMemoryCustomerRepository()
	svc, err := compliance.NewCustomerService(repo, masterKey, audit)
	if err != nil {
		t.Fatalf("unexpected error creating customer service: %v", err)
	}

	account := "GDQP2KPQGKIHYJGXNUIYOMHARUAROT7DJT5ITO2W2WIGISVUMQ6QIBYT"

	// 1. Initial PUT with partial fields -> Expected status NEEDS_INFO
	input := compliance.PutCustomerInput{
		TenantID: "tenant-1",
		Account:  account,
		Fields: map[string]string{
			"first_name": "Ada",
			"last_name":  "Lovelace",
		},
	}

	cust, err := svc.PutCustomer(ctx, input)
	if err != nil {
		t.Fatalf("PutCustomer failed: %v", err)
	}

	if cust.Status != compliance.CustomerStatusNeedsInfo {
		t.Errorf("expected status %s, got %s", compliance.CustomerStatusNeedsInfo, cust.Status)
	}

	// Verify encryption at rest in repository
	rawRec, err := repo.GetByID(ctx, "tenant-1", cust.ID)
	if err != nil {
		t.Fatalf("GetByID on repo failed: %v", err)
	}
	// Raw PII must be encrypted (not plaintext JSON containing "Ada")
	if string(rawRec.EncryptedPII) == "" || string(rawRec.EncryptedPII) == `{"first_name":"Ada","last_name":"Lovelace"}` {
		t.Fatalf("PII is not encrypted in repository: %s", string(rawRec.EncryptedPII))
	}

	// 2. Add email and upload passport binary document -> Expected status PROCESSING
	passportData := []byte("SAMPLE_PASSPORT_IMAGE_BINARY_DATA_12345")
	input2 := compliance.PutCustomerInput{
		ID:       cust.ID,
		TenantID: "tenant-1",
		Account:  account,
		Fields: map[string]string{
			"email_address": "ada@example.com",
		},
		Documents: map[string]*compliance.CustomerDocument{
			compliance.DocFieldPassport: {
				Field:       compliance.DocFieldPassport,
				Filename:    "passport.jpg",
				ContentType: "image/jpeg",
				Data:        passportData,
			},
		},
	}

	cust2, err := svc.PutCustomer(ctx, input2)
	if err != nil {
		t.Fatalf("second PutCustomer failed: %v", err)
	}

	if cust2.Status != compliance.CustomerStatusProcessing {
		t.Errorf("expected status %s, got %s", compliance.CustomerStatusProcessing, cust2.Status)
	}

	// Verify document was encrypted at rest
	rawRec2, err := repo.GetByID(ctx, "tenant-1", cust.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	encDoc := rawRec2.EncryptedDocs[compliance.DocFieldPassport]
	if string(encDoc) == string(passportData) {
		t.Fatalf("document binary was not encrypted at rest")
	}

	// Verify decrypted document retrieval
	fetched, err := svc.GetCustomer(ctx, "tenant-1", cust.ID, "", "", "")
	if err != nil {
		t.Fatalf("GetCustomer failed: %v", err)
	}
	if string(fetched.Documents[compliance.DocFieldPassport].Data) != string(passportData) {
		t.Errorf("decrypted document does not match original data")
	}

	// 3. Status Transition to ACCEPTED
	approved, err := svc.UpdateStatus(ctx, "tenant-1", cust.ID, compliance.CustomerStatusAccepted, "Identity verified")
	if err != nil {
		t.Fatalf("UpdateStatus failed: %v", err)
	}
	if approved.Status != compliance.CustomerStatusAccepted {
		t.Errorf("expected status ACCEPTED, got %s", approved.Status)
	}

	// 4. Status Transition to REJECTED
	rejected, err := svc.UpdateStatus(ctx, "tenant-1", cust.ID, compliance.CustomerStatusRejected, "Fraudulent document")
	if err != nil {
		t.Fatalf("UpdateStatus failed: %v", err)
	}
	if rejected.Status != compliance.CustomerStatusRejected {
		t.Errorf("expected status REJECTED, got %s", rejected.Status)
	}

	// 5. Delete Customer
	err = svc.DeleteCustomer(ctx, "tenant-1", cust.ID, "", "")
	if err != nil {
		t.Fatalf("DeleteCustomer failed: %v", err)
	}

	// Verify deletion
	_, err = svc.GetCustomer(ctx, "tenant-1", cust.ID, "", "", "")
	if err != compliance.ErrCustomerNotFound {
		t.Errorf("expected ErrCustomerNotFound, got %v", err)
	}

	// Verify audit logs were emitted for all operations
	expectedActions := []string{
		"customer.created",
		"customer.updated",
		"customer.status_updated",
		"customer.status_updated",
		"customer.deleted",
	}
	if len(audit.actions) != len(expectedActions) {
		t.Fatalf("expected %d audit actions, got %d: %v", len(expectedActions), len(audit.actions), audit.actions)
	}
	for i, exp := range expectedActions {
		if audit.actions[i] != exp {
			t.Errorf("step %d: expected audit action %s, got %s", i, exp, audit.actions[i])
		}
	}
}

func TestCustomerService_InvalidKey(t *testing.T) {
	_, err := compliance.NewCustomerService(nil, []byte("short-key"), nil)
	if err != compliance.ErrInvalidEncryptionKey {
		t.Errorf("expected ErrInvalidEncryptionKey, got %v", err)
	}
}
