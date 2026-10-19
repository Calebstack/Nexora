package compliance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/google/uuid"
)

// Verification status states per SEP-12 specification and acceptance criteria.
type CustomerStatus string

const (
	CustomerStatusAccepted   CustomerStatus = "ACCEPTED"
	CustomerStatusProcessing CustomerStatus = "PROCESSING"
	CustomerStatusNeedsInfo  CustomerStatus = "NEEDS_INFO"
	CustomerStatusRejected   CustomerStatus = "REJECTED"
)

// IsValid validates whether the verification status is a recognized SEP-12 state.
func (s CustomerStatus) IsValid() bool {
	switch s {
	case CustomerStatusAccepted, CustomerStatusProcessing, CustomerStatusNeedsInfo, CustomerStatusRejected:
		return true
	default:
		return false
	}
}

var (
	ErrCustomerNotFound      = errors.New("customer not found")
	ErrInvalidStatus         = errors.New("invalid customer verification status")
	ErrMissingAccount        = errors.New("customer account or id is required")
	ErrInvalidEncryptionKey  = errors.New("encryption key must be exactly 32 bytes")
	ErrDocumentTooLarge      = errors.New("uploaded document exceeds maximum size limit")
	ErrUnsupportedDocType    = errors.New("unsupported document type")
	ErrCustomerAlreadyExists = errors.New("customer already exists")
)

// Supported binary document upload field names (SEP-9 / SEP-12).
const (
	DocFieldPassport       = "passport"
	DocFieldPhotoIDFront   = "photo_id_front"
	DocFieldPhotoIDBack    = "photo_id_back"
	DocFieldProofOfAddress = "proof_of_address"
	DocFieldNationalID     = "national_id"
)

// CustomerDocument represents a binary KYC document uploaded by the customer.
type CustomerDocument struct {
	Field       string    `json:"field"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	Data        []byte    `json:"-"`
	Checksum    string    `json:"checksum"` // SHA-256 of raw plaintext
	Size        int64     `json:"size"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

// Customer represents the in-memory domain entity for a KYC customer.
type Customer struct {
	ID            string                       `json:"id"`
	TenantID      string                       `json:"tenant_id"`
	Account       string                       `json:"account"`
	Memo          string                       `json:"memo,omitempty"`
	MemoType      string                       `json:"memo_type,omitempty"`
	Type          string                       `json:"type,omitempty"`
	Status        CustomerStatus               `json:"status"`
	StatusMessage string                       `json:"status_message,omitempty"`
	Fields        map[string]string            `json:"fields,omitempty"`
	Documents     map[string]*CustomerDocument `json:"documents,omitempty"`
	CreatedAt     time.Time                    `json:"created_at"`
	UpdatedAt     time.Time                    `json:"updated_at"`
}

// EncryptedCustomerRecord represents the customer persisted at rest with AES-256-GCM encryption.
type EncryptedCustomerRecord struct {
	ID            string            `json:"id"`
	TenantID      string            `json:"tenant_id"`
	Account       string            `json:"account"`
	Memo          string            `json:"memo,omitempty"`
	MemoType      string            `json:"memo_type,omitempty"`
	Type          string            `json:"type,omitempty"`
	Status        CustomerStatus    `json:"status"`
	StatusMessage string            `json:"status_message,omitempty"`
	EncryptedPII  []byte            `json:"encrypted_pii"` // AES-256-GCM of JSON fields
	EncryptedDocs map[string][]byte `json:"encrypted_docs"` // field -> AES-256-GCM encrypted data
	DocMetadata   map[string]DocMeta `json:"doc_metadata"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

type DocMeta struct {
	Field       string    `json:"field"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	Checksum    string    `json:"checksum"`
	Size        int64     `json:"size"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

// CustomerAuditRecorder defines an interface for emitting audit events upon KYC modifications.
type CustomerAuditRecorder interface {
	RecordCustomerAudit(ctx context.Context, action, customerID, tenantID string, metadata map[string]interface{}) error
}

// NopCustomerAuditRecorder is a default no-op audit recorder.
type NopCustomerAuditRecorder struct{}

func (n *NopCustomerAuditRecorder) RecordCustomerAudit(ctx context.Context, action, customerID, tenantID string, metadata map[string]interface{}) error {
	return nil
}

// CustomerRepository defines database access for encrypted customer records.
type CustomerRepository interface {
	Save(ctx context.Context, record *EncryptedCustomerRecord) error
	GetByID(ctx context.Context, tenantID, id string) (*EncryptedCustomerRecord, error)
	GetByAccount(ctx context.Context, tenantID, account, memo, kycType string) (*EncryptedCustomerRecord, error)
	Delete(ctx context.Context, tenantID, id string) error
	DeleteByAccount(ctx context.Context, tenantID, account, memo string) error
	List(ctx context.Context, tenantID string, limit, offset int) ([]*EncryptedCustomerRecord, error)
}

// MemoryCustomerRepository provides a thread-safe, in-memory implementation of CustomerRepository.
type MemoryCustomerRepository struct {
	mu      sync.RWMutex
	byID    map[string]*EncryptedCustomerRecord
	byIndex map[string]string // "tenant:account:memo:type" -> ID
}

func NewMemoryCustomerRepository() *MemoryCustomerRepository {
	return &MemoryCustomerRepository{
		byID:    make(map[string]*EncryptedCustomerRecord),
		byIndex: make(map[string]string),
	}
}

func (r *MemoryCustomerRepository) indexKey(tenantID, account, memo, kycType string) string {
	return fmt.Sprintf("%s:%s:%s:%s", tenantID, account, memo, kycType)
}

func (r *MemoryCustomerRepository) Save(ctx context.Context, record *EncryptedCustomerRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Deep copy to prevent outside mutation
	cp := *record
	cp.EncryptedPII = append([]byte(nil), record.EncryptedPII...)
	cp.EncryptedDocs = make(map[string][]byte, len(record.EncryptedDocs))
	for k, v := range record.EncryptedDocs {
		cp.EncryptedDocs[k] = append([]byte(nil), v...)
	}
	cp.DocMetadata = make(map[string]DocMeta, len(record.DocMetadata))
	for k, v := range record.DocMetadata {
		cp.DocMetadata[k] = v
	}

	r.byID[record.ID] = &cp
	r.byIndex[r.indexKey(record.TenantID, record.Account, record.Memo, record.Type)] = record.ID
	return nil
}

func (r *MemoryCustomerRepository) GetByID(ctx context.Context, tenantID, id string) (*EncryptedCustomerRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rec, ok := r.byID[id]
	if !ok || (tenantID != "" && rec.TenantID != tenantID) {
		return nil, ErrCustomerNotFound
	}
	return rec, nil
}

func (r *MemoryCustomerRepository) GetByAccount(ctx context.Context, tenantID, account, memo, kycType string) (*EncryptedCustomerRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.byIndex[r.indexKey(tenantID, account, memo, kycType)]
	if !ok {
		// Fallback without type if empty
		id, ok = r.byIndex[r.indexKey(tenantID, account, memo, "")]
		if !ok {
			return nil, ErrCustomerNotFound
		}
	}
	return r.byID[id], nil
}

func (r *MemoryCustomerRepository) Delete(ctx context.Context, tenantID, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.byID[id]
	if !ok || (tenantID != "" && rec.TenantID != tenantID) {
		return ErrCustomerNotFound
	}
	delete(r.byIndex, r.indexKey(rec.TenantID, rec.Account, rec.Memo, rec.Type))
	delete(r.byID, id)
	return nil
}

func (r *MemoryCustomerRepository) DeleteByAccount(ctx context.Context, tenantID, account, memo string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var targetID string
	for _, rec := range r.byID {
		if (tenantID == "" || rec.TenantID == tenantID) && rec.Account == account && rec.Memo == memo {
			targetID = rec.ID
			delete(r.byIndex, r.indexKey(rec.TenantID, rec.Account, rec.Memo, rec.Type))
			break
		}
	}
	if targetID == "" {
		return ErrCustomerNotFound
	}
	delete(r.byID, targetID)
	return nil
}

func (r *MemoryCustomerRepository) List(ctx context.Context, tenantID string, limit, offset int) ([]*EncryptedCustomerRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []*EncryptedCustomerRecord
	for _, rec := range r.byID {
		if tenantID == "" || rec.TenantID == tenantID {
			results = append(results, rec)
		}
	}

	if offset >= len(results) {
		return []*EncryptedCustomerRecord{}, nil
	}
	end := offset + limit
	if limit <= 0 || end > len(results) {
		end = len(results)
	}
	return results[offset:end], nil
}

// PutCustomerInput encapsulates the parameters for creating or updating a customer.
type PutCustomerInput struct {
	ID        string                       `json:"id,omitempty"`
	TenantID  string                       `json:"tenant_id"`
	Account   string                       `json:"account"`
	Memo      string                       `json:"memo,omitempty"`
	MemoType  string                       `json:"memo_type,omitempty"`
	Type      string                       `json:"type,omitempty"`
	Fields    map[string]string            `json:"fields"`
	Documents map[string]*CustomerDocument `json:"documents,omitempty"`
}

// CustomerService orchestrates KYC customer operations with AES-256-GCM encryption and audit logs.
type CustomerService interface {
	GetCustomer(ctx context.Context, tenantID, id, account, memo, kycType string) (*Customer, error)
	PutCustomer(ctx context.Context, input PutCustomerInput) (*Customer, error)
	DeleteCustomer(ctx context.Context, tenantID, id, account, memo string) error
	UpdateStatus(ctx context.Context, tenantID, id string, status CustomerStatus, message string) (*Customer, error)
}

type customerService struct {
	repo      CustomerRepository
	masterKey []byte
	audit     CustomerAuditRecorder
}

// NewCustomerService constructs a production CustomerService.
func NewCustomerService(repo CustomerRepository, masterKey []byte, audit CustomerAuditRecorder) (CustomerService, error) {
	if len(masterKey) != 32 {
		return nil, ErrInvalidEncryptionKey
	}
	if repo == nil {
		repo = NewMemoryCustomerRepository()
	}
	if audit == nil {
		audit = &NopCustomerAuditRecorder{}
	}
	return &customerService{
		repo:      repo,
		masterKey: masterKey,
		audit:     audit,
	}, nil
}

// encryptCustomer converts a plaintext Customer into an EncryptedCustomerRecord.
func (s *customerService) encryptCustomer(c *Customer) (*EncryptedCustomerRecord, error) {
	rawFields, err := json.Marshal(c.Fields)
	if err != nil {
		return nil, fmt.Errorf("marshal customer fields: %w", err)
	}

	encPII, err := crypto.Encrypt(rawFields, s.masterKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt pii fields: %w", err)
	}

	encDocs := make(map[string][]byte, len(c.Documents))
	docMeta := make(map[string]DocMeta, len(c.Documents))

	for field, doc := range c.Documents {
		if len(doc.Data) > 0 {
			encData, err := crypto.Encrypt(doc.Data, s.masterKey)
			if err != nil {
				return nil, fmt.Errorf("encrypt document %s: %w", field, err)
			}
			encDocs[field] = encData
		}
		docMeta[field] = DocMeta{
			Field:       doc.Field,
			Filename:    doc.Filename,
			ContentType: doc.ContentType,
			Checksum:    doc.Checksum,
			Size:        doc.Size,
			UploadedAt:  doc.UploadedAt,
		}
	}

	return &EncryptedCustomerRecord{
		ID:            c.ID,
		TenantID:      c.TenantID,
		Account:       c.Account,
		Memo:          c.Memo,
		MemoType:      c.MemoType,
		Type:          c.Type,
		Status:        c.Status,
		StatusMessage: c.StatusMessage,
		EncryptedPII:  encPII,
		EncryptedDocs: encDocs,
		DocMetadata:   docMeta,
		CreatedAt:     c.CreatedAt,
		UpdatedAt:     c.UpdatedAt,
	}, nil
}

// decryptCustomer converts an EncryptedCustomerRecord back into a plaintext Customer entity.
func (s *customerService) decryptCustomer(rec *EncryptedCustomerRecord) (*Customer, error) {
	decPII, err := crypto.Decrypt(rec.EncryptedPII, s.masterKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt pii fields: %w", err)
	}

	fields := make(map[string]string)
	if err := json.Unmarshal(decPII, &fields); err != nil {
		return nil, fmt.Errorf("unmarshal pii fields: %w", err)
	}

	docs := make(map[string]*CustomerDocument, len(rec.DocMetadata))
	for field, meta := range rec.DocMetadata {
		doc := &CustomerDocument{
			Field:       meta.Field,
			Filename:    meta.Filename,
			ContentType: meta.ContentType,
			Checksum:    meta.Checksum,
			Size:        meta.Size,
			UploadedAt:  meta.UploadedAt,
		}
		if encData, ok := rec.EncryptedDocs[field]; ok && len(encData) > 0 {
			decData, err := crypto.Decrypt(encData, s.masterKey)
			if err != nil {
				return nil, fmt.Errorf("decrypt document %s: %w", field, err)
			}
			doc.Data = decData
		}
		docs[field] = doc
	}

	return &Customer{
		ID:            rec.ID,
		TenantID:      rec.TenantID,
		Account:       rec.Account,
		Memo:          rec.Memo,
		MemoType:      rec.MemoType,
		Type:          rec.Type,
		Status:        rec.Status,
		StatusMessage: rec.StatusMessage,
		Fields:        fields,
		Documents:     docs,
		CreatedAt:     rec.CreatedAt,
		UpdatedAt:     rec.UpdatedAt,
	}, nil
}

func (s *customerService) GetCustomer(ctx context.Context, tenantID, id, account, memo, kycType string) (*Customer, error) {
	var rec *EncryptedCustomerRecord
	var err error

	if id != "" {
		rec, err = s.repo.GetByID(ctx, tenantID, id)
	} else if account != "" {
		rec, err = s.repo.GetByAccount(ctx, tenantID, account, memo, kycType)
	} else {
		return nil, ErrMissingAccount
	}

	if err != nil {
		return nil, err
	}
	return s.decryptCustomer(rec)
}

func (s *customerService) PutCustomer(ctx context.Context, input PutCustomerInput) (*Customer, error) {
	if input.Account == "" && input.ID == "" {
		return nil, ErrMissingAccount
	}

	now := time.Now().UTC()
	var customer *Customer
	var isNew bool

	// Try locating existing customer
	if input.ID != "" {
		c, err := s.GetCustomer(ctx, input.TenantID, input.ID, "", "", "")
		if err == nil {
			customer = c
		}
	} else if input.Account != "" {
		c, err := s.GetCustomer(ctx, input.TenantID, "", input.Account, input.Memo, input.Type)
		if err == nil {
			customer = c
		}
	}

	if customer == nil {
		isNew = true
		customerID := input.ID
		if customerID == "" {
			customerID = uuid.New().String()
		}
		customer = &Customer{
			ID:        customerID,
			TenantID:  input.TenantID,
			Account:   input.Account,
			Memo:      input.Memo,
			MemoType:  input.MemoType,
			Type:      input.Type,
			Status:    CustomerStatusNeedsInfo,
			Fields:    make(map[string]string),
			Documents: make(map[string]*CustomerDocument),
			CreatedAt: now,
			UpdatedAt: now,
		}
	} else {
		customer.UpdatedAt = now
		if input.Type != "" {
			customer.Type = input.Type
		}
	}

	// Update text fields
	for k, v := range input.Fields {
		customer.Fields[k] = strings.TrimSpace(v)
	}

	// Ingest & hash documents
	for k, doc := range input.Documents {
		if doc != nil {
			if len(doc.Data) > 10*1024*1024 { // 10MB limit
				return nil, ErrDocumentTooLarge
			}
			h := sha256.Sum256(doc.Data)
			doc.Checksum = hex.EncodeToString(h[:])
			doc.Size = int64(len(doc.Data))
			doc.UploadedAt = now
			customer.Documents[k] = doc
		}
	}

	// Determine verification status
	customer.Status = evaluateVerificationStatus(customer)

	// Encrypt at rest and persist
	encRec, err := s.encryptCustomer(customer)
	if err != nil {
		return nil, fmt.Errorf("encrypt customer record: %w", err)
	}

	if err := s.repo.Save(ctx, encRec); err != nil {
		return nil, fmt.Errorf("save customer: %w", err)
	}

	// Emit audit log
	action := "customer.updated"
	if isNew {
		action = "customer.created"
	}
	auditMeta := map[string]interface{}{
		"account":        customer.Account,
		"memo":           customer.Memo,
		"status":         string(customer.Status),
		"documents_count": len(customer.Documents),
		"fields_count":   len(customer.Fields),
	}
	_ = s.audit.RecordCustomerAudit(ctx, action, customer.ID, customer.TenantID, auditMeta)

	return customer, nil
}

func (s *customerService) DeleteCustomer(ctx context.Context, tenantID, id, account, memo string) error {
	var targetID string
	var targetAccount string

	if id != "" {
		c, err := s.GetCustomer(ctx, tenantID, id, "", "", "")
		if err != nil {
			return err
		}
		targetID = c.ID
		targetAccount = c.Account
		if err := s.repo.Delete(ctx, tenantID, id); err != nil {
			return err
		}
	} else if account != "" {
		c, err := s.GetCustomer(ctx, tenantID, "", account, memo, "")
		if err != nil {
			return err
		}
		targetID = c.ID
		targetAccount = c.Account
		if err := s.repo.DeleteByAccount(ctx, tenantID, account, memo); err != nil {
			return err
		}
	} else {
		return ErrMissingAccount
	}

	// Emit audit log
	auditMeta := map[string]interface{}{
		"account": targetAccount,
		"memo":    memo,
	}
	_ = s.audit.RecordCustomerAudit(ctx, "customer.deleted", targetID, tenantID, auditMeta)
	return nil
}

func (s *customerService) UpdateStatus(ctx context.Context, tenantID, id string, status CustomerStatus, message string) (*Customer, error) {
	if !status.IsValid() {
		return nil, ErrInvalidStatus
	}

	customer, err := s.GetCustomer(ctx, tenantID, id, "", "", "")
	if err != nil {
		return nil, err
	}

	customer.Status = status
	customer.StatusMessage = message
	customer.UpdatedAt = time.Now().UTC()

	encRec, err := s.encryptCustomer(customer)
	if err != nil {
		return nil, fmt.Errorf("encrypt customer record: %w", err)
	}

	if err := s.repo.Save(ctx, encRec); err != nil {
		return nil, fmt.Errorf("save updated customer: %w", err)
	}

	auditMeta := map[string]interface{}{
		"old_status": string(customer.Status),
		"new_status": string(status),
		"reason":     message,
	}
	_ = s.audit.RecordCustomerAudit(ctx, "customer.status_updated", customer.ID, customer.TenantID, auditMeta)

	return customer, nil
}

// evaluateVerificationStatus assesses the completeness of identity documents and PII fields.
func evaluateVerificationStatus(c *Customer) CustomerStatus {
	// If explicitly set to REJECTED or ACCEPTED by compliance officer, retain it
	if c.Status == CustomerStatusRejected {
		return CustomerStatusRejected
	}
	if c.Status == CustomerStatusAccepted {
		return CustomerStatusAccepted
	}

	// Check core required fields
	firstName := c.Fields["first_name"]
	lastName := c.Fields["last_name"]
	email := c.Fields["email_address"]

	hasIDDoc := false
	for field := range c.Documents {
		if field == DocFieldPhotoIDFront || field == DocFieldPassport || field == DocFieldNationalID {
			hasIDDoc = true
			break
		}
	}

	if firstName != "" && lastName != "" && email != "" && hasIDDoc {
		// All required KYC documents and identity fields submitted; ready for automated/manual processing
		return CustomerStatusProcessing
	}

	return CustomerStatusNeedsInfo
}
