package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	newMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestProductFound(t *testing.T) {
	rec := get(t, "/products/product-789")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Laptop") {
		t.Fatalf("unexpected response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestProductNotFound(t *testing.T) {
	if rec := get(t, "/products/nope"); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestInactiveCustomer(t *testing.T) {
	rec := get(t, "/customers/customer-999")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"active":false`) {
		t.Fatalf("unexpected response: %d %s", rec.Code, rec.Body.String())
	}
}
