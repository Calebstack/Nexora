package anchor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
)

var (
	ErrCorridorSchemaNotFound = errors.New("corridor schema not found")
	ErrInvalidCorridorSchema  = errors.New("invalid corridor schema")
)

// maxCorridorSchemaBytes bounds a registered schema document.
const maxCorridorSchemaBytes = 64 << 10

var (
	corridorKeyPattern  = regexp.MustCompile(`^[A-Z0-9][A-Z0-9:_-]{1,31}$`)
	customerTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	// ifscPattern is the RBI IFSC layout: 4-letter bank code, a reserved
	// 0, then a 6-character branch code.
	ifscPattern = regexp.MustCompile(`^[A-Z]{4}0[A-Z0-9]{6}$`)
)

// CorridorSchema is one immutable version of the JSON Schema that SEP-12
// customer data must satisfy for a corridor and SEP-12 customer type (for
// example "sep31-receiver"). Exactly one version per key is active.
type CorridorSchema struct {
	ID           string
	TenantID     string
	Corridor     string
	CustomerType string
	Version      int
	Schema       json.RawMessage
	Active       bool
	CreatedBy    string
	CreatedAt    time.Time
}

// CorridorSchemaStore persists the append-only version history.
type CorridorSchemaStore interface {
	// CreateVersion assigns the next version number, stores s and makes it
	// the active version, atomically.
	CreateVersion(ctx context.Context, s *CorridorSchema) error
	// Get returns a version, or the active one when version is 0.
	Get(ctx context.Context, tenantID, corridor, customerType string, version int) (*CorridorSchema, error)
	// History returns every version, newest first.
	History(ctx context.Context, tenantID, corridor, customerType string) ([]*CorridorSchema, error)
	// Activate makes an existing version the active one.
	Activate(ctx context.Context, tenantID, corridor, customerType string, version int) error
}

// FieldError is one localized validation failure for a customer field.
type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SchemaValidation is the outcome of validating a customer payload.
type SchemaValidation struct {
	Valid         bool         `json:"valid"`
	SchemaVersion int          `json:"schema_version"`
	Locale        string       `json:"locale"`
	Errors        []FieldError `json:"errors,omitempty"`
}

// SchemaRegistryConfig tunes caching. Zero values fall back to defaults.
type SchemaRegistryConfig struct {
	// ActiveTTL bounds how long another instance can serve a superseded
	// active version after an update (default 30s).
	ActiveTTL time.Duration
	Now       func() time.Time
}

// SchemaRegistry registers, versions and enforces corridor customer field
// schemas at runtime, so new corridor requirements need no redeployment.
type SchemaRegistry struct {
	store CorridorSchemaStore
	cfg   SchemaRegistryConfig

	mu       sync.RWMutex
	compiled map[string]*jsonschema.Schema // schema version ID -> compiled; versions are immutable
	active   map[schemaKey]activeEntry
}

type schemaKey struct{ tenantID, corridor, customerType string }

type activeEntry struct {
	schema  *CorridorSchema
	expires time.Time
}

func NewSchemaRegistry(store CorridorSchemaStore, cfg SchemaRegistryConfig) *SchemaRegistry {
	if cfg.ActiveTTL <= 0 {
		cfg.ActiveTTL = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &SchemaRegistry{store: store, cfg: cfg, compiled: make(map[string]*jsonschema.Schema), active: make(map[schemaKey]activeEntry)}
}

// Register validates and stores a new version and makes it active.
func (r *SchemaRegistry) Register(ctx context.Context, tenantID, corridor, customerType string, schema json.RawMessage, actor string) (*CorridorSchema, error) {
	if err := validateSchemaKey(corridor, customerType); err != nil {
		return nil, err
	}
	if len(schema) > maxCorridorSchemaBytes {
		return nil, fmt.Errorf("%w: schema exceeds %d bytes", ErrInvalidCorridorSchema, maxCorridorSchemaBytes)
	}
	compiled, err := compileCorridorSchema(schema)
	if err != nil {
		return nil, err
	}
	s := &CorridorSchema{TenantID: tenantID, Corridor: corridor, CustomerType: customerType, Schema: schema, CreatedBy: actor, CreatedAt: r.cfg.Now().UTC()}
	if err := r.store.CreateVersion(ctx, s); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.compiled[s.ID] = compiled
	delete(r.active, schemaKey{tenantID, corridor, customerType})
	r.mu.Unlock()
	return s, nil
}

// Activate rolls the active version to an earlier (or later) one.
func (r *SchemaRegistry) Activate(ctx context.Context, tenantID, corridor, customerType string, version int) error {
	if err := r.store.Activate(ctx, tenantID, corridor, customerType, version); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.active, schemaKey{tenantID, corridor, customerType})
	r.mu.Unlock()
	return nil
}

// History returns every version, newest first.
func (r *SchemaRegistry) History(ctx context.Context, tenantID, corridor, customerType string) ([]*CorridorSchema, error) {
	return r.store.History(ctx, tenantID, corridor, customerType)
}

// Get returns a version, or the active one when version is 0. The active
// version is served from a TTL cache: O(1) on a hit.
func (r *SchemaRegistry) Get(ctx context.Context, tenantID, corridor, customerType string, version int) (*CorridorSchema, error) {
	if version != 0 {
		return r.store.Get(ctx, tenantID, corridor, customerType, version)
	}
	key := schemaKey{tenantID, corridor, customerType}
	now := r.cfg.Now()
	r.mu.RLock()
	entry, ok := r.active[key]
	r.mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return entry.schema, nil
	}
	s, err := r.store.Get(ctx, tenantID, corridor, customerType, 0)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.active[key] = activeEntry{schema: s, expires: now.Add(r.cfg.ActiveTTL)}
	r.mu.Unlock()
	return s, nil
}

// Validate checks a customer payload (a JSON object of SEP-9 fields) against
// the active version, or a pinned version for backwards compatibility, and
// localizes errors for the best match of acceptLanguage. O(n) in the payload
// size plus the schema's keywords.
func (r *SchemaRegistry) Validate(ctx context.Context, tenantID, corridor, customerType string, version int, payload []byte, acceptLanguage string) (*SchemaValidation, error) {
	s, err := r.Get(ctx, tenantID, corridor, customerType, version)
	if err != nil {
		return nil, err
	}
	compiled, err := r.compiledFor(s)
	if err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%w: customer payload is not valid JSON", ErrInvalidCorridorSchema)
	}
	if _, ok := doc.(map[string]any); !ok {
		return nil, fmt.Errorf("%w: customer payload must be a JSON object", ErrInvalidCorridorSchema)
	}

	locale := MatchLocale(acceptLanguage)
	result := &SchemaValidation{Valid: true, SchemaVersion: s.Version, Locale: locale}
	var verr *jsonschema.ValidationError
	if err := compiled.Validate(doc); err != nil {
		if !errors.As(err, &verr) {
			return nil, fmt.Errorf("validate customer payload: %w", err)
		}
		result.Valid = false
		result.Errors = localizeErrors(verr, locale)
	}
	return result, nil
}

func (r *SchemaRegistry) compiledFor(s *CorridorSchema) (*jsonschema.Schema, error) {
	r.mu.RLock()
	c, ok := r.compiled[s.ID]
	r.mu.RUnlock()
	if ok {
		return c, nil
	}
	c, err := compileCorridorSchema(s.Schema)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.compiled[s.ID] = c
	r.mu.Unlock()
	return c, nil
}

func validateSchemaKey(corridor, customerType string) error {
	if !corridorKeyPattern.MatchString(corridor) {
		return fmt.Errorf("%w: corridor must match %s", ErrInvalidCorridorSchema, corridorKeyPattern)
	}
	if !customerTypePattern.MatchString(customerType) {
		return fmt.Errorf("%w: customer type must match %s", ErrInvalidCorridorSchema, customerTypePattern)
	}
	return nil
}

// rejectLoader refuses every external $ref so a tenant schema can never make
// the server read files or call the network.
type rejectLoader struct{}

func (rejectLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external schema references are not allowed: %s", url)
}

// compileCorridorSchema compiles a JSON Schema (draft 2020-12 unless it
// declares $schema) with format assertions on. Draft 2020-12 otherwise
// treats "format" as an annotation only, which would not enforce IFSC or
// CLABE. The top level must describe an object.
func compileCorridorSchema(raw json.RawMessage) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCorridorSchema, err)
	}
	obj, ok := doc.(map[string]any)
	if !ok || obj["type"] != "object" {
		return nil, fmt.Errorf(`%w: top level must be {"type": "object", ...}`, ErrInvalidCorridorSchema)
	}

	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(rejectLoader{})
	c.RegisterFormat(&jsonschema.Format{Name: "ifsc", Validate: validateIFSC})
	c.RegisterFormat(&jsonschema.Format{Name: "clabe", Validate: validateCLABE})
	const url = "urn:nexora:corridor-schema"
	if err := c.AddResource(url, doc); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCorridorSchema, err)
	}
	compiled, err := c.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCorridorSchema, err)
	}
	return compiled, nil
}

// validateIFSC checks an Indian Financial System Code. Non-strings are left
// to the "type" keyword, per JSON Schema format semantics.
func validateIFSC(v any) error {
	s, ok := v.(string)
	if !ok {
		return nil
	}
	if !ifscPattern.MatchString(s) {
		return errors.New("not an IFSC code")
	}
	return nil
}

// validateCLABE checks a Mexican CLABE: 18 digits whose last digit is the
// control digit over the first 17 with weights 3, 7, 1 repeating (each
// product mod 10, sum mod 10, then (10 - sum) mod 10).
func validateCLABE(v any) error {
	s, ok := v.(string)
	if !ok {
		return nil
	}
	if len(s) != 18 {
		return errors.New("CLABE must have 18 digits")
	}
	weights := [3]int{3, 7, 1}
	sum := 0
	for i := 0; i < 18; i++ {
		if s[i] < '0' || s[i] > '9' {
			return errors.New("CLABE must contain only digits")
		}
		if i < 17 {
			sum += (int(s[i]-'0') * weights[i%3]) % 10
		}
	}
	if want := (10 - sum%10) % 10; int(s[17]-'0') != want {
		return errors.New("CLABE control digit mismatch")
	}
	return nil
}

// Sep12Fields renders a schema's top-level properties as SEP-12 `fields`
// (type string|binary|number|date, description, choices, optional), so
// clients can discover corridor requirements through GET /customer.
func Sep12Fields(raw json.RawMessage) (map[string]Sep12Field, error) {
	var schema struct {
		Required   []string                   `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCorridorSchema, err)
	}
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	fields := make(map[string]Sep12Field, len(schema.Properties))
	for name, rawProp := range schema.Properties {
		var prop struct {
			Type            any    `json:"type"`
			Format          string `json:"format"`
			ContentEncoding string `json:"contentEncoding"`
			Description     string `json:"description"`
			Enum            []any  `json:"enum"`
		}
		if err := json.Unmarshal(rawProp, &prop); err != nil {
			continue // boolean schemas carry no field metadata
		}
		sepType, ok := sep12Type(prop.Type, prop.Format, prop.ContentEncoding)
		if !ok {
			continue // objects and arrays have no SEP-12 field type
		}
		f := Sep12Field{Type: sepType, Description: prop.Description, Optional: !required[name]}
		for _, choice := range prop.Enum {
			f.Choices = append(f.Choices, fmt.Sprint(choice))
		}
		fields[name] = f
	}
	return fields, nil
}

func sep12Type(t any, format, encoding string) (string, bool) {
	name, _ := t.(string)
	if list, ok := t.([]any); ok {
		for _, item := range list {
			if s, _ := item.(string); s != "null" {
				name = s
				break
			}
		}
	}
	switch name {
	case "number", "integer":
		return "number", true
	case "string":
		switch {
		case format == "date":
			return "date", true
		case format == "binary" || encoding == "base64":
			return "binary", true
		}
		return "string", true
	case "boolean":
		return "string", true
	}
	return "", false
}

// supportedLocales are the languages validation messages are written in.
var supportedLocales = []language.Tag{language.English, language.Spanish, language.French, language.Portuguese}

var localeMatcher = language.NewMatcher(supportedLocales)

// MatchLocale picks the best supported language for an Accept-Language
// header, defaulting to English.
func MatchLocale(acceptLanguage string) string {
	tags, _, err := language.ParseAcceptLanguage(acceptLanguage)
	if err != nil || len(tags) == 0 {
		return "en"
	}
	_, index, confidence := localeMatcher.Match(tags...)
	if confidence == language.No {
		return "en"
	}
	base, _ := supportedLocales[index].Base()
	return base.String()
}

// validationMessages holds one template per error code and locale. {field}
// is the customer field and {detail} the code-specific value.
var validationMessages = map[string]map[string]string{
	"missing": {
		"en": "{field} is required.", "es": "{field} es obligatorio.",
		"fr": "{field} est obligatoire.", "pt": "{field} é obrigatório.",
	},
	"invalid_type": {
		"en": "{field} must be of type {detail}.", "es": "{field} debe ser de tipo {detail}.",
		"fr": "{field} doit être de type {detail}.", "pt": "{field} deve ser do tipo {detail}.",
	},
	"invalid_format": {
		"en": "{field} is not a valid {detail}.", "es": "{field} no es un {detail} válido.",
		"fr": "{field} n'est pas un {detail} valide.", "pt": "{field} não é um {detail} válido.",
	},
	"pattern_mismatch": {
		"en": "{field} has an invalid format.", "es": "{field} tiene un formato no válido.",
		"fr": "{field} a un format invalide.", "pt": "{field} tem um formato inválido.",
	},
	"too_short": {
		"en": "{field} must be at least {detail} characters.", "es": "{field} debe tener al menos {detail} caracteres.",
		"fr": "{field} doit contenir au moins {detail} caractères.", "pt": "{field} deve ter pelo menos {detail} caracteres.",
	},
	"too_long": {
		"en": "{field} must be at most {detail} characters.", "es": "{field} debe tener como máximo {detail} caracteres.",
		"fr": "{field} doit contenir au plus {detail} caractères.", "pt": "{field} deve ter no máximo {detail} caracteres.",
	},
	"invalid_choice": {
		"en": "{field} must be one of: {detail}.", "es": "{field} debe ser uno de: {detail}.",
		"fr": "{field} doit être l'une des valeurs : {detail}.", "pt": "{field} deve ser um de: {detail}.",
	},
	"out_of_range": {
		"en": "{field} is outside the allowed range.", "es": "{field} está fuera del rango permitido.",
		"fr": "{field} est en dehors de la plage autorisée.", "pt": "{field} está fora do intervalo permitido.",
	},
	"not_allowed": {
		"en": "{field} is not an accepted field.", "es": "{field} no es un campo aceptado.",
		"fr": "{field} n'est pas un champ accepté.", "pt": "{field} não é um campo aceito.",
	},
	"invalid": {
		"en": "{field} is invalid.", "es": "{field} no es válido.",
		"fr": "{field} n'est pas valide.", "pt": "{field} não é válido.",
	},
}

// formatLabels names custom formats in messages.
var formatLabels = map[string]string{"ifsc": "IFSC code", "clabe": "CLABE"}

// localizeErrors flattens the validator's error tree into one FieldError per
// failing field and keyword, sorted by field then code.
func localizeErrors(root *jsonschema.ValidationError, locale string) []FieldError {
	var out []FieldError
	seen := make(map[string]bool)
	add := func(field, code, detail string) {
		if key := field + "\x00" + code; !seen[key] {
			seen[key] = true
			msg := strings.NewReplacer("{field}", field, "{detail}", detail).Replace(validationMessages[code][locale])
			out = append(out, FieldError{Field: field, Code: code, Message: msg})
		}
	}
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, c := range e.Causes {
				walk(c)
			}
			return
		}
		field := strings.Join(e.InstanceLocation, ".")
		switch k := e.ErrorKind.(type) {
		case *kind.Required:
			for _, name := range k.Missing {
				add(joinField(field, name), "missing", "")
			}
		case *kind.AdditionalProperties:
			for _, name := range k.Properties {
				add(joinField(field, name), "not_allowed", "")
			}
		case *kind.Type:
			add(field, "invalid_type", strings.Join(k.Want, " | "))
		case *kind.Format:
			label := formatLabels[k.Want]
			if label == "" {
				label = k.Want
			}
			add(field, "invalid_format", label)
		case *kind.Pattern:
			add(field, "pattern_mismatch", "")
		case *kind.MinLength:
			add(field, "too_short", strconv.Itoa(k.Want))
		case *kind.MaxLength:
			add(field, "too_long", strconv.Itoa(k.Want))
		case *kind.Enum:
			choices := make([]string, len(k.Want))
			for i, c := range k.Want {
				choices[i] = fmt.Sprint(c)
			}
			add(field, "invalid_choice", strings.Join(choices, ", "))
		case *kind.Minimum, *kind.Maximum, *kind.ExclusiveMinimum, *kind.ExclusiveMaximum:
			add(field, "out_of_range", "")
		default:
			add(field, "invalid", "")
		}
	}
	walk(root)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Field != out[j].Field {
			return out[i].Field < out[j].Field
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func joinField(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "." + name
}

// SchemaRegistryHandler is the tenant-administrator REST API.
type SchemaRegistryHandler struct {
	registry *SchemaRegistry
}

func NewSchemaRegistryHandler(registry *SchemaRegistry) *SchemaRegistryHandler {
	return &SchemaRegistryHandler{registry: registry}
}

// Routes mounts, under the caller's tenant-admin prefix (e.g. /corridors):
//
//	GET  /{corridor}/schemas?type=                    version history
//	POST /{corridor}/schemas                          register a new active version
//	GET  /{corridor}/schemas/active?type=             active version + SEP-12 fields
//	GET  /{corridor}/schemas/{version}?type=          one version + SEP-12 fields
//	POST /{corridor}/schemas/{version}/activate?type= roll the active version
//	POST /{corridor}/validate                         validate a customer payload
func (h *SchemaRegistryHandler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Get("/{corridor}/schemas", h.history)
		r.Post("/{corridor}/schemas", h.register)
		r.Get("/{corridor}/schemas/active", h.get)
		r.Get("/{corridor}/schemas/{version}", h.get)
		r.Post("/{corridor}/schemas/{version}/activate", h.activate)
		r.Post("/{corridor}/validate", h.validate)
	}
}

type corridorSchemaResponse struct {
	Corridor     string                `json:"corridor"`
	CustomerType string                `json:"customer_type"`
	Version      int                   `json:"version"`
	Active       bool                  `json:"active"`
	Schema       json.RawMessage       `json:"schema"`
	Fields       map[string]Sep12Field `json:"fields,omitempty"`
	CreatedBy    string                `json:"created_by,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
}

func toCorridorSchemaResponse(s *CorridorSchema, withFields bool) corridorSchemaResponse {
	resp := corridorSchemaResponse{Corridor: s.Corridor, CustomerType: s.CustomerType, Version: s.Version, Active: s.Active,
		Schema: s.Schema, CreatedBy: s.CreatedBy, CreatedAt: s.CreatedAt}
	if withFields {
		resp.Fields, _ = Sep12Fields(s.Schema)
	}
	return resp
}

func (h *SchemaRegistryHandler) history(w http.ResponseWriter, r *http.Request) {
	versions, err := h.registry.History(r.Context(), tenant.IDFromContext(r.Context()), chi.URLParam(r, "corridor"), r.URL.Query().Get("type"))
	if err != nil {
		writeSchemaError(w, err)
		return
	}
	out := make([]corridorSchemaResponse, 0, len(versions))
	for _, v := range versions {
		out = append(out, toCorridorSchemaResponse(v, false))
	}
	api.JSON(w, http.StatusOK, map[string]any{"versions": out})
}

type registerSchemaRequest struct {
	CustomerType string          `json:"customer_type"`
	Schema       json.RawMessage `json:"schema"`
}

func (h *SchemaRegistryHandler) register(w http.ResponseWriter, r *http.Request) {
	var req registerSchemaRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxCorridorSchemaBytes+4096)).Decode(&req); err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	s, err := h.registry.Register(r.Context(), tenant.IDFromContext(r.Context()), chi.URLParam(r, "corridor"),
		req.CustomerType, req.Schema, api.ActorFromContext(r.Context()))
	if err != nil {
		writeSchemaError(w, err)
		return
	}
	api.JSON(w, http.StatusCreated, toCorridorSchemaResponse(s, true))
}

func (h *SchemaRegistryHandler) get(w http.ResponseWriter, r *http.Request) {
	version, ok := versionParam(w, r)
	if !ok {
		return
	}
	s, err := h.registry.Get(r.Context(), tenant.IDFromContext(r.Context()), chi.URLParam(r, "corridor"), r.URL.Query().Get("type"), version)
	if err != nil {
		writeSchemaError(w, err)
		return
	}
	api.JSON(w, http.StatusOK, toCorridorSchemaResponse(s, true))
}

func (h *SchemaRegistryHandler) activate(w http.ResponseWriter, r *http.Request) {
	version, ok := versionParam(w, r)
	if !ok {
		return
	}
	if version == 0 {
		api.BadRequest(w, "version must be a positive integer")
		return
	}
	if err := h.registry.Activate(r.Context(), tenant.IDFromContext(r.Context()), chi.URLParam(r, "corridor"), r.URL.Query().Get("type"), version); err != nil {
		writeSchemaError(w, err)
		return
	}
	api.JSON(w, http.StatusOK, map[string]any{"active_version": version})
}

type validateCustomerRequest struct {
	CustomerType string          `json:"customer_type"`
	Version      int             `json:"version,omitempty"`
	Customer     json.RawMessage `json:"customer"`
}

func (h *SchemaRegistryHandler) validate(w http.ResponseWriter, r *http.Request) {
	var req validateCustomerRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || len(req.Customer) == 0 {
		api.BadRequest(w, "body must contain a customer object")
		return
	}
	result, err := h.registry.Validate(r.Context(), tenant.IDFromContext(r.Context()), chi.URLParam(r, "corridor"),
		req.CustomerType, req.Version, req.Customer, r.Header.Get("Accept-Language"))
	if err != nil {
		writeSchemaError(w, err)
		return
	}
	w.Header().Set("Content-Language", result.Locale)
	status := http.StatusOK
	if !result.Valid {
		status = http.StatusUnprocessableEntity
	}
	api.JSON(w, status, result)
}

// versionParam returns 0 for the "active" route.
func versionParam(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := chi.URLParam(r, "version")
	if raw == "" {
		return 0, true
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		api.BadRequest(w, "version must be a positive integer")
		return 0, false
	}
	return v, true
}

func writeSchemaError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCorridorSchemaNotFound):
		api.NotFound(w, "corridor schema not found")
	case errors.Is(err, ErrInvalidCorridorSchema):
		api.BadRequest(w, err.Error())
	default:
		api.InternalError(w, err)
	}
}
