package anchor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// memSchemaStore mirrors CorridorSchemaRepo semantics in memory.
type memSchemaStore struct {
	mu       sync.Mutex
	versions map[schemaKey][]*CorridorSchema
	gets     int
}

func newMemSchemaStore() *memSchemaStore {
	return &memSchemaStore{versions: map[schemaKey][]*CorridorSchema{}}
}

func (s *memSchemaStore) CreateVersion(_ context.Context, cs *CorridorSchema) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := schemaKey{cs.TenantID, cs.Corridor, cs.CustomerType}
	for _, v := range s.versions[key] {
		v.Active = false
	}
	cs.ID, cs.Version, cs.Active = uuid.NewString(), len(s.versions[key])+1, true
	cp := *cs
	s.versions[key] = append(s.versions[key], &cp)
	return nil
}

func (s *memSchemaStore) Get(_ context.Context, tenantID, corridor, customerType string, version int) (*CorridorSchema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	for _, v := range s.versions[schemaKey{tenantID, corridor, customerType}] {
		if (version == 0 && v.Active) || v.Version == version {
			cp := *v
			return &cp, nil
		}
	}
	return nil, ErrCorridorSchemaNotFound
}

func (s *memSchemaStore) History(_ context.Context, tenantID, corridor, customerType string) ([]*CorridorSchema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.versions[schemaKey{tenantID, corridor, customerType}]
	if len(versions) == 0 {
		return nil, ErrCorridorSchemaNotFound
	}
	out := make([]*CorridorSchema, len(versions))
	for i, v := range versions {
		cp := *v
		out[len(versions)-1-i] = &cp
	}
	return out, nil
}

func (s *memSchemaStore) Activate(_ context.Context, tenantID, corridor, customerType string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.versions[schemaKey{tenantID, corridor, customerType}]
	if version < 1 || version > len(versions) {
		return ErrCorridorSchemaNotFound
	}
	for _, v := range versions {
		v.Active = v.Version == version
	}
	return nil
}

const (
	schemaTenant = "tenant-1"
	inrCorridor  = "USDC:INR"
	mxnCorridor  = "USDC:MXN"
	receiverType = "sep31-receiver"
)

// indiaSchema requires SEP-9 bank fields plus a custom IFSC field.
const indiaSchema = `{
  "type": "object",
  "required": ["first_name", "last_name", "bank_account_number", "ifsc_code"],
  "properties": {
    "first_name": {"type": "string", "minLength": 1, "description": "Receiver first name"},
    "last_name": {"type": "string", "minLength": 1},
    "bank_account_number": {"type": "string", "pattern": "^[0-9]{9,18}$"},
    "ifsc_code": {"type": "string", "format": "ifsc", "description": "Indian Financial System Code"},
    "bank_account_type": {"type": "string", "enum": ["checking", "savings"]},
    "birth_date": {"type": "string", "format": "date"},
    "photo_id_front": {"type": "string", "contentEncoding": "base64"},
    "amount_limit": {"type": "number", "maximum": 10000}
  }
}`

const mexicoSchema = `{
  "type": "object",
  "required": ["clabe_number"],
  "properties": {"clabe_number": {"type": "string", "format": "clabe"}},
  "additionalProperties": false
}`

func newTestRegistry(t *testing.T) *SchemaRegistry {
	t.Helper()
	r := NewSchemaRegistry(newMemSchemaStore(), SchemaRegistryConfig{})
	if _, err := r.Register(context.Background(), schemaTenant, inrCorridor, receiverType, json.RawMessage(indiaSchema), "admin-1"); err != nil {
		t.Fatalf("register india: %v", err)
	}
	if _, err := r.Register(context.Background(), schemaTenant, mxnCorridor, receiverType, json.RawMessage(mexicoSchema), "admin-1"); err != nil {
		t.Fatalf("register mexico: %v", err)
	}
	return r
}

func validate(t *testing.T, r *SchemaRegistry, corridor string, version int, payload, lang string) *SchemaValidation {
	t.Helper()
	res, err := r.Validate(context.Background(), schemaTenant, corridor, receiverType, version, []byte(payload), lang)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return res
}

func codes(errs []FieldError) []string {
	out := make([]string, len(errs))
	for i, e := range errs {
		out[i] = e.Field + ":" + e.Code
	}
	return out
}

func TestSchemaRegistry_ValidatesIFSCPayloads(t *testing.T) {
	r := newTestRegistry(t)

	ok := validate(t, r, inrCorridor, 0, `{"first_name":"Asha","last_name":"Rao","bank_account_number":"123456789012","ifsc_code":"HDFC0001234"}`, "")
	if !ok.Valid || ok.SchemaVersion != 1 || len(ok.Errors) != 0 {
		t.Fatalf("valid payload = %+v", ok)
	}

	bad := validate(t, r, inrCorridor, 0, `{"first_name":"","bank_account_number":"12ab","ifsc_code":"HDFC1001234","bank_account_type":"current","amount_limit":20000}`, "en")
	got := codes(bad.Errors)
	want := []string{
		"amount_limit:out_of_range", "bank_account_number:pattern_mismatch", "bank_account_type:invalid_choice",
		"first_name:too_short", "ifsc_code:invalid_format", "last_name:missing",
	}
	if bad.Valid || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("errors = %v, want %v", got, want)
	}
	for _, e := range bad.Errors {
		switch e.Field {
		case "ifsc_code":
			if e.Message != "ifsc_code is not a valid IFSC code." {
				t.Errorf("ifsc message = %q", e.Message)
			}
		case "bank_account_type":
			if e.Message != "bank_account_type must be one of: checking, savings." {
				t.Errorf("enum message = %q", e.Message)
			}
		}
	}
}

func TestSchemaRegistry_LocalizedMessages(t *testing.T) {
	r := newTestRegistry(t)
	payload := `{"clabe_number":"032180000118359718"}` // wrong control digit
	tests := map[string]string{
		"es-MX,es;q=0.9,en;q=0.5": "clabe_number no es un CLABE válido.",
		"fr-CA":                   "clabe_number n'est pas un CLABE valide.",
		"pt-BR":                   "clabe_number não é um CLABE válido.",
		"de-DE":                   "clabe_number is not a valid CLABE.",
		"":                        "clabe_number is not a valid CLABE.",
	}
	for lang, want := range tests {
		res := validate(t, r, mxnCorridor, 0, payload, lang)
		if res.Valid || len(res.Errors) != 1 || res.Errors[0].Message != want {
			t.Errorf("%q: %+v, want %q", lang, res.Errors, want)
		}
	}

	missing := validate(t, r, mxnCorridor, 0, `{"extra":"x"}`, "es")
	if got := codes(missing.Errors); strings.Join(got, ",") != "clabe_number:missing,extra:not_allowed" {
		t.Fatalf("errors = %v", got)
	}
	if missing.Errors[0].Message != "clabe_number es obligatorio." || missing.Locale != "es" {
		t.Fatalf("spanish missing message = %+v", missing)
	}
}

func TestValidateCLABE(t *testing.T) {
	// 032180000118359719 is the worked example from the CLABE article:
	// products 0,1,2,3,6,0,0,0,0,3,7,8,9,5,9,1,7 sum to 61, control 10-1 = 9.
	if err := validateCLABE("032180000118359719"); err != nil {
		t.Fatalf("worked example rejected: %v", err)
	}
	for _, bad := range []string{"032180000118359710", "03218000011835971", "03218000011835971A", "0321800001183597190"} {
		if validateCLABE(bad) == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if validateCLABE(42) != nil {
		t.Error("non-strings are left to the type keyword")
	}
}

func TestValidateIFSC(t *testing.T) {
	for _, ok := range []string{"HDFC0001234", "SBIN0ABC123"} {
		if validateIFSC(ok) != nil {
			t.Errorf("%s rejected", ok)
		}
	}
	for _, bad := range []string{"HDFC1001234", "hdfc0001234", "HDF00001234", "HDFC000123", "HDFC00012345"} {
		if validateIFSC(bad) == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestSchemaRegistry_VersionHistoryAndPinnedValidation(t *testing.T) {
	r := newTestRegistry(t)
	ctx := context.Background()
	v2 := `{"type":"object","required":["clabe_number","bank_name"],
	        "properties":{"clabe_number":{"type":"string","format":"clabe"},"bank_name":{"type":"string"}}}`
	created, err := r.Register(ctx, schemaTenant, mxnCorridor, receiverType, json.RawMessage(v2), "admin-2")
	if err != nil || created.Version != 2 || !created.Active {
		t.Fatalf("register v2 = %+v, %v", created, err)
	}

	payload := `{"clabe_number":"032180000118359719"}`
	if res := validate(t, r, mxnCorridor, 0, payload, ""); res.Valid || res.SchemaVersion != 2 {
		t.Fatalf("active v2 must require bank_name: %+v", res)
	}
	if res := validate(t, r, mxnCorridor, 1, payload, ""); !res.Valid || res.SchemaVersion != 1 {
		t.Fatalf("pinned v1 must still accept the old payload: %+v", res)
	}

	history, err := r.History(ctx, schemaTenant, mxnCorridor, receiverType)
	if err != nil || len(history) != 2 || history[0].Version != 2 || history[1].Version != 1 || history[1].Active {
		t.Fatalf("history = %+v, %v", history, err)
	}

	if err := r.Activate(ctx, schemaTenant, mxnCorridor, receiverType, 1); err != nil {
		t.Fatal(err)
	}
	if res := validate(t, r, mxnCorridor, 0, payload, ""); !res.Valid || res.SchemaVersion != 1 {
		t.Fatalf("after rollback the active version is v1: %+v", res)
	}
	if err := r.Activate(ctx, schemaTenant, mxnCorridor, receiverType, 9); !errors.Is(err, ErrCorridorSchemaNotFound) {
		t.Fatalf("activate unknown err = %v", err)
	}
	if _, err := r.Validate(ctx, "other-tenant", mxnCorridor, receiverType, 0, []byte(payload), ""); !errors.Is(err, ErrCorridorSchemaNotFound) {
		t.Fatalf("tenants must not see each other's schemas: %v", err)
	}
}

func TestSchemaRegistry_ActiveCacheTTLAcrossInstances(t *testing.T) {
	store := newMemSchemaStore()
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	writer := NewSchemaRegistry(store, SchemaRegistryConfig{Now: clock})
	reader := NewSchemaRegistry(store, SchemaRegistryConfig{Now: clock, ActiveTTL: 30 * time.Second})
	ctx := context.Background()
	if _, err := writer.Register(ctx, schemaTenant, mxnCorridor, receiverType, json.RawMessage(mexicoSchema), ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		validate(t, reader, mxnCorridor, 0, `{"clabe_number":"032180000118359719"}`, "")
	}
	if store.gets != 1 {
		t.Fatalf("store reads = %d, want 1 (active version cached)", store.gets)
	}

	loose := `{"type":"object","properties":{"clabe_number":{"type":"string"}}}`
	if _, err := writer.Register(ctx, schemaTenant, mxnCorridor, receiverType, json.RawMessage(loose), ""); err != nil {
		t.Fatal(err)
	}
	if res := validate(t, reader, mxnCorridor, 0, `{}`, ""); res.SchemaVersion != 1 {
		t.Fatal("within the TTL the other instance may still serve v1")
	}
	now = now.Add(31 * time.Second)
	if res := validate(t, reader, mxnCorridor, 0, `{}`, ""); res.SchemaVersion != 2 || !res.Valid {
		t.Fatalf("after the TTL the update is live without a redeploy: %+v", res)
	}
}

func TestSchemaRegistry_RejectsUnsafeOrInvalidSchemas(t *testing.T) {
	r := NewSchemaRegistry(newMemSchemaStore(), SchemaRegistryConfig{})
	ctx := context.Background()
	tests := map[string]struct{ corridor, ctype, schema string }{
		"array top level":       {inrCorridor, receiverType, `{"type":"array"}`},
		"not json":              {inrCorridor, receiverType, `{`},
		"bad keyword value":     {inrCorridor, receiverType, `{"type":"object","properties":{"a":{"minLength":"x"}}}`},
		"external file ref":     {inrCorridor, receiverType, `{"type":"object","properties":{"a":{"$ref":"file:///etc/passwd"}}}`},
		"external http ref":     {inrCorridor, receiverType, `{"type":"object","properties":{"a":{"$ref":"https://example.com/s.json"}}}`},
		"oversized":             {inrCorridor, receiverType, `{"type":"object","description":"` + strings.Repeat("x", maxCorridorSchemaBytes) + `"}`},
		"lowercase corridor":    {"usdc:inr", receiverType, `{"type":"object"}`},
		"invalid customer type": {inrCorridor, "SEP31 Receiver", `{"type":"object"}`},
	}
	for name, tc := range tests {
		if _, err := r.Register(ctx, schemaTenant, tc.corridor, tc.ctype, json.RawMessage(tc.schema), ""); !errors.Is(err, ErrInvalidCorridorSchema) {
			t.Errorf("%s: err = %v, want ErrInvalidCorridorSchema", name, err)
		}
	}

	r2 := newTestRegistry(t)
	if _, err := r2.Validate(ctx, schemaTenant, inrCorridor, receiverType, 0, []byte(`["not","object"]`), ""); !errors.Is(err, ErrInvalidCorridorSchema) {
		t.Fatalf("non-object payload err = %v", err)
	}
}

func TestSep12Fields(t *testing.T) {
	fields, err := Sep12Fields(json.RawMessage(indiaSchema))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Sep12Field{
		"first_name":          {Type: "string", Description: "Receiver first name"},
		"last_name":           {Type: "string"},
		"bank_account_number": {Type: "string"},
		"ifsc_code":           {Type: "string", Description: "Indian Financial System Code"},
		"bank_account_type":   {Type: "string", Optional: true, Choices: []string{"checking", "savings"}},
		"birth_date":          {Type: "date", Optional: true},
		"photo_id_front":      {Type: "binary", Optional: true},
		"amount_limit":        {Type: "number", Optional: true},
	}
	if len(fields) != len(want) {
		t.Fatalf("fields = %+v", fields)
	}
	for name, w := range want {
		g := fields[name]
		if g.Type != w.Type || g.Description != w.Description || g.Optional != w.Optional || strings.Join(g.Choices, ",") != strings.Join(w.Choices, ",") {
			t.Errorf("%s = %+v, want %+v", name, g, w)
		}
	}
}

func TestMatchLocale(t *testing.T) {
	for header, want := range map[string]string{
		"es-MX,es;q=0.9": "es", "pt-BR": "pt", "fr": "fr", "en-GB": "en",
		"de-DE": "en", "": "en", "not a header;;": "en", "hi-IN,es;q=0.5": "es",
	} {
		if got := MatchLocale(header); got != want {
			t.Errorf("MatchLocale(%q) = %s, want %s", header, got, want)
		}
	}
}

func TestSchemaRegistryHandler(t *testing.T) {
	r := NewSchemaRegistry(newMemSchemaStore(), SchemaRegistryConfig{})
	router := chi.NewRouter()
	router.Route("/corridors", NewSchemaRegistryHandler(r).Routes())
	do := func(method, path, body, lang string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req = req.WithContext(tenant.WithID(req.Context(), schemaTenant))
		if lang != "" {
			req.Header.Set("Accept-Language", lang)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	rec := do(http.MethodPost, "/corridors/USDC:MXN/schemas", `{"customer_type":"sep31-receiver","schema":`+mexicoSchema+`}`, "")
	if rec.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	var created corridorSchemaResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if created.Version != 1 || !created.Active || created.Fields["clabe_number"].Type != "string" {
		t.Fatalf("created = %+v", created)
	}

	_ = do(http.MethodPost, "/corridors/USDC:MXN/schemas", `{"customer_type":"sep31-receiver","schema":{"type":"object"}}`, "")
	rec = do(http.MethodGet, "/corridors/USDC:MXN/schemas?type=sep31-receiver", "", "")
	var hist struct {
		Versions []corridorSchemaResponse `json:"versions"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &hist)
	if rec.Code != http.StatusOK || len(hist.Versions) != 2 || hist.Versions[0].Version != 2 {
		t.Fatalf("history: %d %s", rec.Code, rec.Body)
	}

	if rec = do(http.MethodPost, "/corridors/USDC:MXN/schemas/1/activate?type=sep31-receiver", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("activate: %d %s", rec.Code, rec.Body)
	}
	rec = do(http.MethodGet, "/corridors/USDC:MXN/schemas/active?type=sep31-receiver", "", "")
	var active corridorSchemaResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &active)
	if rec.Code != http.StatusOK || active.Version != 1 {
		t.Fatalf("active: %d %s", rec.Code, rec.Body)
	}
	if rec = do(http.MethodGet, "/corridors/USDC:MXN/schemas/2?type=sep31-receiver", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("get version: %d", rec.Code)
	}

	rec = do(http.MethodPost, "/corridors/USDC:MXN/validate", `{"customer_type":"sep31-receiver","customer":{"clabe_number":"123"}}`, "es-MX")
	var result SchemaValidation
	_ = json.Unmarshal(rec.Body.Bytes(), &result)
	if rec.Code != http.StatusUnprocessableEntity || rec.Header().Get("Content-Language") != "es" || result.Valid || result.Errors[0].Message != "clabe_number no es un CLABE válido." {
		t.Fatalf("validate invalid: %d %s", rec.Code, rec.Body)
	}
	rec = do(http.MethodPost, "/corridors/USDC:MXN/validate", `{"customer_type":"sep31-receiver","customer":{"clabe_number":"032180000118359719"}}`, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"valid":true`) {
		t.Fatalf("validate valid: %d %s", rec.Code, rec.Body)
	}

	errorCases := []struct {
		method, path, body string
		code               int
	}{
		{http.MethodPost, "/corridors/USDC:MXN/schemas", `{"customer_type":"sep31-receiver","schema":{"type":"string"}}`, http.StatusBadRequest},
		{http.MethodPost, "/corridors/USDC:MXN/schemas", `nope`, http.StatusBadRequest},
		{http.MethodGet, "/corridors/USDC:KES/schemas/active?type=sep31-receiver", "", http.StatusNotFound},
		{http.MethodGet, "/corridors/USDC:MXN/schemas/zero?type=sep31-receiver", "", http.StatusBadRequest},
		{http.MethodPost, "/corridors/USDC:MXN/schemas/7/activate?type=sep31-receiver", "", http.StatusNotFound},
		{http.MethodPost, "/corridors/USDC:MXN/validate", `{"customer_type":"sep31-receiver"}`, http.StatusBadRequest},
	}
	for _, tc := range errorCases {
		if rec := do(tc.method, tc.path, tc.body, ""); rec.Code != tc.code {
			t.Errorf("%s %s: %d, want %d (%s)", tc.method, tc.path, rec.Code, tc.code, rec.Body)
		}
	}
}
