package anchor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/compliance"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
)

// SEP-12 Standard KYC Field Definitions
type Sep12Field struct {
	Type        string   `json:"type"`                  // string, binary, number, date
	Description string   `json:"description"`           // Human readable description
	Optional    bool     `json:"optional,omitempty"`    // Defaults to false
	Choices     []string `json:"choices,omitempty"`     // Valid choices if enumerated
	Status      string   `json:"status,omitempty"`      // Status if provided
	Error       string   `json:"error,omitempty"`       // Field error if rejected
}

type Sep12ProvidedField struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	Status      string `json:"status"` // ACCEPTED, PROCESSING, NEEDS_INFO, REJECTED
	Error       string `json:"error,omitempty"`
}

type GetCustomerResponse struct {
	ID             string                         `json:"id,omitempty"`
	Status         string                         `json:"status"` // ACCEPTED, PROCESSING, NEEDS_INFO, REJECTED
	Fields         map[string]Sep12Field          `json:"fields,omitempty"`
	ProvidedFields map[string]Sep12ProvidedField  `json:"provided_fields,omitempty"`
	Message        string                         `json:"message,omitempty"`
}

type PutCustomerResponse struct {
	ID string `json:"id"`
}

type Sep12Handler struct {
	customerSvc compliance.CustomerService
}

func NewSep12Handler(customerSvc compliance.CustomerService) *Sep12Handler {
	return &Sep12Handler{customerSvc: customerSvc}
}

// Routes returns a sub-router for SEP-12 KYC endpoints.
func (h *Sep12Handler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Get("/", h.GetCustomer)
		r.Put("/", h.PutCustomer)
		r.Delete("/", h.DeleteCustomer)
		r.Delete("/{account}", h.DeleteCustomer)
	}
}

// GetCustomer handles GET /customer per SEP-12 specification.
func (h *Sep12Handler) GetCustomer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)

	id := r.URL.Query().Get("id")
	account := r.URL.Query().Get("account")
	memo := r.URL.Query().Get("memo")
	memoType := r.URL.Query().Get("memo_type")
	kycType := r.URL.Query().Get("type")
	_ = memoType

	// If neither account nor id was provided in query, attempt to extract authenticated account
	if id == "" && account == "" {
		if authAcct := extractAccountFromContext(ctx); authAcct != "" {
			account = authAcct
		}
	}

	cust, err := h.customerSvc.GetCustomer(ctx, tenantID, id, account, memo, kycType)
	if err != nil {
		if errors.Is(err, compliance.ErrCustomerNotFound) || errors.Is(err, compliance.ErrMissingAccount) {
			// Per SEP-12: If customer is not yet registered, return 200 with status NEEDS_INFO and required fields
			api.JSON(w, http.StatusOK, GetCustomerResponse{
				Status:  string(compliance.CustomerStatusNeedsInfo),
				Fields:  defaultRequiredFields(kycType),
				Message: "Customer identity registration required",
			})
			return
		}
		api.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	provided := make(map[string]Sep12ProvidedField)
	for k := range cust.Fields {
		provided[k] = Sep12ProvidedField{
			Type:        "string",
			Description: fieldDescription(k),
			Status:      string(cust.Status),
		}
	}
	for docField := range cust.Documents {
		provided[docField] = Sep12ProvidedField{
			Type:        "binary",
			Description: docDescription(docField),
			Status:      string(cust.Status),
		}
	}

	var neededFields map[string]Sep12Field
	if cust.Status == compliance.CustomerStatusNeedsInfo {
		neededFields = computeMissingFields(cust, kycType)
	}

	api.JSON(w, http.StatusOK, GetCustomerResponse{
		ID:             cust.ID,
		Status:         string(cust.Status),
		Fields:         neededFields,
		ProvidedFields: provided,
		Message:        cust.StatusMessage,
	})
}

// PutCustomer handles PUT /customer supporting multipart/form-data, json, and urlencoded.
func (h *Sep12Handler) PutCustomer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)

	input := compliance.PutCustomerInput{
		TenantID:  tenantID,
		Fields:    make(map[string]string),
		Documents: make(map[string]*compliance.CustomerDocument),
	}

	contentType := r.Header.Get("Content-Type")

	if strings.HasPrefix(contentType, "multipart/form-data") {
		// Limit to 32MB max multipart memory
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.Error(w, http.StatusBadRequest, "INVALID_MULTIPART", "failed to parse multipart form: "+err.Error())
			return
		}

		for k, values := range r.MultipartForm.Value {
			if len(values) == 0 {
				continue
			}
			val := strings.TrimSpace(values[0])
			switch k {
			case "id":
				input.ID = val
			case "account":
				input.Account = val
			case "memo":
				input.Memo = val
			case "memo_type":
				input.MemoType = val
			case "type":
				input.Type = val
			default:
				input.Fields[k] = val
			}
		}

		// Process binary document files
		for fieldName, fileHeaders := range r.MultipartForm.File {
			if len(fileHeaders) == 0 {
				continue
			}
			fh := fileHeaders[0]
			file, err := fh.Open()
			if err != nil {
				api.Error(w, http.StatusBadRequest, "FILE_OPEN_ERROR", "failed to open uploaded file: "+err.Error())
				return
			}
			fileData, err := io.ReadAll(io.LimitReader(file, 10<<20)) // 10MB limit per document
			file.Close()
			if err != nil {
				api.Error(w, http.StatusBadRequest, "FILE_READ_ERROR", "failed to read uploaded file: "+err.Error())
				return
			}

			cType := fh.Header.Get("Content-Type")
			if cType == "" {
				cType = "application/octet-stream"
			}

			input.Documents[fieldName] = &compliance.CustomerDocument{
				Field:       fieldName,
				Filename:    filepath.Base(fh.Filename),
				ContentType: cType,
				Data:        fileData,
			}
		}
	} else if strings.HasPrefix(contentType, "application/json") {
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.Error(w, http.StatusBadRequest, "INVALID_JSON", "invalid JSON request body")
			return
		}

		for k, v := range body {
			strVal := fmt.Sprintf("%v", v)
			switch k {
			case "id":
				input.ID = strVal
			case "account":
				input.Account = strVal
			case "memo":
				input.Memo = strVal
			case "memo_type":
				input.MemoType = strVal
			case "type":
				input.Type = strVal
			default:
				input.Fields[k] = strVal
			}
		}
	} else {
		// x-www-form-urlencoded
		if err := r.ParseForm(); err != nil {
			api.Error(w, http.StatusBadRequest, "INVALID_FORM", "failed to parse form data")
			return
		}
		for k, values := range r.Form {
			if len(values) == 0 {
				continue
			}
			val := strings.TrimSpace(values[0])
			switch k {
			case "id":
				input.ID = val
			case "account":
				input.Account = val
			case "memo":
				input.Memo = val
			case "memo_type":
				input.MemoType = val
			case "type":
				input.Type = val
			default:
				input.Fields[k] = val
			}
		}
	}

	if input.Account == "" && input.ID == "" {
		if authAcct := extractAccountFromContext(ctx); authAcct != "" {
			input.Account = authAcct
		}
	}

	if input.Account == "" && input.ID == "" {
		api.Error(w, http.StatusBadRequest, "MISSING_ACCOUNT", "account or id is required")
		return
	}

	cust, err := h.customerSvc.PutCustomer(ctx, input)
	if err != nil {
		if errors.Is(err, compliance.ErrDocumentTooLarge) {
			api.Error(w, http.StatusBadRequest, "DOCUMENT_TOO_LARGE", err.Error())
			return
		}
		api.Error(w, http.StatusBadRequest, "CUSTOMER_UPDATE_FAILED", err.Error())
		return
	}

	api.JSON(w, http.StatusAccepted, PutCustomerResponse{
		ID: cust.ID,
	})
}

// DeleteCustomer handles DELETE /customer and DELETE /customer/{account}.
func (h *Sep12Handler) DeleteCustomer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)

	account := chi.URLParam(r, "account")
	if account == "" {
		account = r.URL.Query().Get("account")
	}
	id := r.URL.Query().Get("id")
	memo := r.URL.Query().Get("memo")

	if account == "" && id == "" {
		if authAcct := extractAccountFromContext(ctx); authAcct != "" {
			account = authAcct
		}
	}

	if account == "" && id == "" {
		api.Error(w, http.StatusBadRequest, "MISSING_IDENTIFIER", "account or id is required for deletion")
		return
	}

	err := h.customerSvc.DeleteCustomer(ctx, tenantID, id, account, memo)
	if err != nil {
		if errors.Is(err, compliance.ErrCustomerNotFound) {
			api.Error(w, http.StatusNotFound, "NOT_FOUND", "customer not found")
			return
		}
		api.Error(w, http.StatusInternalServerError, "DELETE_FAILED", err.Error())
		return
	}

	api.JSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
	})
}

func defaultRequiredFields(kycType string) map[string]Sep12Field {
	return map[string]Sep12Field{
		"first_name": {
			Type:        "string",
			Description: "Customer's legal first name",
			Optional:    false,
		},
		"last_name": {
			Type:        "string",
			Description: "Customer's legal last name",
			Optional:    false,
		},
		"email_address": {
			Type:        "string",
			Description: "Customer's primary email address",
			Optional:    false,
		},
		compliance.DocFieldPhotoIDFront: {
			Type:        "binary",
			Description: "Front of government-issued photo ID (passport, national ID)",
			Optional:    false,
		},
	}
}

func computeMissingFields(c *compliance.Customer, kycType string) map[string]Sep12Field {
	needed := make(map[string]Sep12Field)
	for k, def := range defaultRequiredFields(kycType) {
		if def.Type == "binary" {
			if _, hasDoc := c.Documents[k]; !hasDoc {
				needed[k] = def
			}
		} else {
			if val, hasField := c.Fields[k]; !hasField || val == "" {
				needed[k] = def
			}
		}
	}
	return needed
}

func fieldDescription(field string) string {
	switch field {
	case "first_name":
		return "Legal first name"
	case "last_name":
		return "Legal last name"
	case "email_address":
		return "Primary email address"
	case "mobile_number":
		return "Mobile phone number"
	case "address":
		return "Residential address"
	default:
		return strings.ReplaceAll(field, "_", " ")
	}
}

func docDescription(doc string) string {
	switch doc {
	case compliance.DocFieldPassport:
		return "Passport identity document"
	case compliance.DocFieldPhotoIDFront:
		return "Front of photo identification card"
	case compliance.DocFieldPhotoIDBack:
		return "Back of photo identification card"
	case compliance.DocFieldProofOfAddress:
		return "Utility bill or proof of residential address"
	default:
		return "Verification document (" + doc + ")"
	}
}

func extractAccountFromContext(ctx context.Context) string {
	// Extract subject or account if authenticated via SEP-10 JWT
	if val := ctx.Value("stellar_account"); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

// ----------------------------------------------------------------------
// SEP-12 Client Implementation
// ----------------------------------------------------------------------

type Sep12Client struct {
	httpClient *http.Client
}

func NewSep12Client(httpClient *http.Client) *Sep12Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Sep12Client{httpClient: httpClient}
}

type CustomerQuery struct {
	ID            string
	Account       string
	Memo          string
	MemoType      string
	Type          string
	TransactionID string
	Lang          string
}

// GetCustomer queries the anchor KYC server for customer status and fields.
func (c *Sep12Client) GetCustomer(ctx context.Context, kycServer, jwt string, q CustomerQuery) (*GetCustomerResponse, error) {
	endpoint, err := url.Parse(strings.TrimSuffix(kycServer, "/") + "/customer")
	if err != nil {
		return nil, fmt.Errorf("invalid kyc server url: %w", err)
	}

	params := endpoint.Query()
	if q.ID != "" {
		params.Set("id", q.ID)
	}
	if q.Account != "" {
		params.Set("account", q.Account)
	}
	if q.Memo != "" {
		params.Set("memo", q.Memo)
	}
	if q.MemoType != "" {
		params.Set("memo_type", q.MemoType)
	}
	if q.Type != "" {
		params.Set("type", q.Type)
	}
	if q.TransactionID != "" {
		params.Set("transaction_id", q.TransactionID)
	}
	if q.Lang != "" {
		params.Set("lang", q.Lang)
	}
	endpoint.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build get customer request: %w", err)
	}
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute get customer: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read get customer response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("get customer failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var out GetCustomerResponse
	if err := json.Unmarshal(bodyBytes, &out); err != nil {
		return nil, fmt.Errorf("decode get customer response: %w", err)
	}
	return &out, nil
}

type PutCustomerRequest struct {
	ID       string
	Account  string
	Memo     string
	MemoType string
	Type     string
	Fields   map[string]string
}

type DocumentFile struct {
	Filename    string
	ContentType string
	Data        []byte
}

// PutCustomer submits KYC fields and binary document files to the anchor KYC server.
func (c *Sep12Client) PutCustomer(ctx context.Context, kycServer, jwt string, reqData PutCustomerRequest, docs map[string]*DocumentFile) (*PutCustomerResponse, error) {
	targetURL := strings.TrimSuffix(kycServer, "/") + "/customer"

	var bodyBuf bytes.Buffer
	writer := multipart.NewWriter(&bodyBuf)

	if reqData.ID != "" {
		_ = writer.WriteField("id", reqData.ID)
	}
	if reqData.Account != "" {
		_ = writer.WriteField("account", reqData.Account)
	}
	if reqData.Memo != "" {
		_ = writer.WriteField("memo", reqData.Memo)
	}
	if reqData.MemoType != "" {
		_ = writer.WriteField("memo_type", reqData.MemoType)
	}
	if reqData.Type != "" {
		_ = writer.WriteField("type", reqData.Type)
	}

	for k, v := range reqData.Fields {
		_ = writer.WriteField(k, v)
	}

	for fieldName, doc := range docs {
		if doc != nil {
			part, err := writer.CreateFormFile(fieldName, doc.Filename)
			if err != nil {
				return nil, fmt.Errorf("create form file for %s: %w", fieldName, err)
			}
			if _, err := part.Write(doc.Data); err != nil {
				return nil, fmt.Errorf("write form file for %s: %w", fieldName, err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, &bodyBuf)
	if err != nil {
		return nil, fmt.Errorf("build put customer request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute put customer: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read put customer response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("put customer failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var out PutCustomerResponse
	if err := json.Unmarshal(bodyBytes, &out); err != nil {
		return nil, fmt.Errorf("decode put customer response: %w", err)
	}
	return &out, nil
}

// DeleteCustomer requests the removal of a customer KYC record from the anchor.
func (c *Sep12Client) DeleteCustomer(ctx context.Context, kycServer, jwt, account, memo string) error {
	targetURL := strings.TrimSuffix(kycServer, "/") + "/customer"
	if account != "" {
		targetURL += "/" + account
	}
	if memo != "" {
		targetURL += "?memo=" + url.QueryEscape(memo)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, targetURL, nil)
	if err != nil {
		return fmt.Errorf("build delete customer request: %w", err)
	}
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute delete customer: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete customer failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
