package anchor_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/Lumen-Nexora/Nexora/internal/compliance"
	"github.com/go-chi/chi/v5"
)

func setupSep12TestServer(t *testing.T) (*httptest.Server, compliance.CustomerService) {
	t.Helper()
	masterKey := make([]byte, 32)
	_, _ = rand.Read(masterKey)

	repo := compliance.NewMemoryCustomerRepository()
	custSvc, err := compliance.NewCustomerService(repo, masterKey, nil)
	if err != nil {
		t.Fatalf("failed to create customer service: %v", err)
	}

	h := anchor.NewSep12Handler(custSvc)
	r := chi.NewRouter()
	r.Route("/customer", h.Routes())

	srv := httptest.NewServer(r)
	return srv, custSvc
}

func TestSep12_GetCustomer_Unregistered(t *testing.T) {
	srv, _ := setupSep12TestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/customer?account=GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var data anchor.GetCustomerResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if data.Status != "NEEDS_INFO" {
		t.Errorf("expected status NEEDS_INFO, got %s", data.Status)
	}
	if len(data.Fields) == 0 {
		t.Errorf("expected required fields list, got empty")
	}
}

func TestSep12_PutCustomer_MultipartDocuments(t *testing.T) {
	srv, custSvc := setupSep12TestServer(t)
	defer srv.Close()

	account := "GDQP2KPQGKIHYJGXNUIYOMHARUAROT7DJT5ITO2W2WIGISVUMQ6QIBYT"

	// Construct multipart request with text fields and binary passport
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("account", account)
	_ = writer.WriteField("first_name", "Marie")
	_ = writer.WriteField("last_name", "Curie")
	_ = writer.WriteField("email_address", "marie@curie.org")

	// Binary file part
	filePart, err := writer.CreateFormFile("passport", "passport.png")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	fakePassportBytes := []byte("PASSPORT_IMAGE_RAW_BINARY_BYTES_MOCK")
	_, _ = filePart.Write(fakePassportBytes)

	// Another binary file: proof of address
	addrPart, err := writer.CreateFormFile("proof_of_address", "utility_bill.pdf")
	if err != nil {
		t.Fatalf("CreateFormFile for address failed: %v", err)
	}
	fakeAddressBytes := []byte("PDF_UTILITY_BILL_BINARY_BYTES_MOCK")
	_, _ = addrPart.Write(fakeAddressBytes)

	_ = writer.Close()

	req, err := http.NewRequest(http.MethodPut, srv.URL+"/customer", &body)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202/200, got %d: %s", resp.StatusCode, string(b))
	}

	var putResp anchor.PutCustomerResponse
	if err := json.NewDecoder(resp.Body).Decode(&putResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if putResp.ID == "" {
		t.Fatalf("expected non-empty customer id")
	}

	// Now GET /customer should return PROCESSING since all required docs are submitted
	getResp, err := http.Get(srv.URL + "/customer?id=" + putResp.ID)
	if err != nil {
		t.Fatalf("GET /customer failed: %v", err)
	}
	defer getResp.Body.Close()

	var getData anchor.GetCustomerResponse
	if err := json.NewDecoder(getResp.Body).Decode(&getData); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if getData.Status != "PROCESSING" {
		t.Errorf("expected status PROCESSING, got %s", getData.Status)
	}
	if getData.ProvidedFields["passport"].Type != "binary" {
		t.Errorf("expected binary provided field for passport, got %v", getData.ProvidedFields["passport"])
	}

	// Verify compliance service can retrieve decrypted documents
	cust, err := custSvc.GetCustomer(context.Background(), "", putResp.ID, "", "", "")
	if err != nil {
		t.Fatalf("GetCustomer from service failed: %v", err)
	}
	if string(cust.Documents["passport"].Data) != string(fakePassportBytes) {
		t.Errorf("decrypted passport data mismatch")
	}
	if string(cust.Documents["proof_of_address"].Data) != string(fakeAddressBytes) {
		t.Errorf("decrypted proof_of_address data mismatch")
	}

	// Delete customer via DELETE /customer/{account}
	delReq, err := http.NewRequest(http.MethodDelete, srv.URL+"/customer/"+account, nil)
	if err != nil {
		t.Fatalf("build delete request failed: %v", err)
	}
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("delete request failed: %v", err)
	}
	defer delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for delete, got %d", delResp.StatusCode)
	}

	// After deletion, GET /customer should return NEEDS_INFO again
	getAgain, err := http.Get(srv.URL + "/customer?account=" + account)
	if err != nil {
		t.Fatalf("get after delete failed: %v", err)
	}
	defer getAgain.Body.Close()
	var postDelData anchor.GetCustomerResponse
	_ = json.NewDecoder(getAgain.Body).Decode(&postDelData)
	if postDelData.Status != "NEEDS_INFO" {
		t.Errorf("expected status NEEDS_INFO after delete, got %s", postDelData.Status)
	}
}

func TestSep12_ClientIntegration(t *testing.T) {
	srv, _ := setupSep12TestServer(t)
	defer srv.Close()

	client := anchor.NewSep12Client(nil)
	account := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

	// Test client.GetCustomer (unregistered)
	qResp, err := client.GetCustomer(context.Background(), srv.URL, "mock-jwt", anchor.CustomerQuery{
		Account: account,
	})
	if err != nil {
		t.Fatalf("client.GetCustomer failed: %v", err)
	}
	if qResp.Status != "NEEDS_INFO" {
		t.Errorf("expected NEEDS_INFO, got %s", qResp.Status)
	}

	// Test client.PutCustomer with documents
	docFiles := map[string]*anchor.DocumentFile{
		"photo_id_front": {
			Filename:    "id_card.jpg",
			ContentType: "image/jpeg",
			Data:        []byte("NATIONAL_ID_CARD_BINARY_MOCK"),
		},
	}
	putResp, err := client.PutCustomer(context.Background(), srv.URL, "mock-jwt", anchor.PutCustomerRequest{
		Account: account,
		Fields: map[string]string{
			"first_name":    "Nikola",
			"last_name":     "Tesla",
			"email_address": "nikola@tesla.org",
		},
	}, docFiles)
	if err != nil {
		t.Fatalf("client.PutCustomer failed: %v", err)
	}
	if putResp.ID == "" {
		t.Errorf("expected non-empty customer id from client")
	}

	// Test client.DeleteCustomer
	err = client.DeleteCustomer(context.Background(), srv.URL, "mock-jwt", account, "")
	if err != nil {
		t.Fatalf("client.DeleteCustomer failed: %v", err)
	}
}

func TestSep12_PutCustomer_JSON(t *testing.T) {
	srv, _ := setupSep12TestServer(t)
	defer srv.Close()

	account := "GCFXHS4GXL6BVUCXBWXGTITROWLVYXQKQLF4YH5O5JT3YZXCYPAFBJZB"
	payload := `{"account":"` + account + `","first_name":"Rosalind","last_name":"Franklin"}`

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/customer", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT JSON failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 202/200, got %d", resp.StatusCode)
	}
}
