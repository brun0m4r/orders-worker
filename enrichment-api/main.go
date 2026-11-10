// Enrichment API: catálogo de productos y clientes (datos en memoria para la prueba).
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

type Product struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
	Category    string  `json:"category"`
}

type Customer struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Active bool   `json:"active"`
}

var products = map[string]Product{
	"product-789": {"product-789", "Laptop", "Laptop 14\" 16GB RAM", 999, "computers"},
	"product-790": {"product-790", "Mouse", "Mouse inalámbrico", 25, "accessories"},
	"product-791": {"product-791", "Monitor", "Monitor 27\" 4K", 450, "displays"},
	"product-792": {"product-792", "Teclado", "Teclado mecánico", 80, "accessories"},
}

var customers = map[string]Customer{
	"customer-456": {"customer-456", "Ana Pérez", "ana@example.com", true},
	"customer-457": {"customer-457", "Luis Gómez", "luis@example.com", true},
	"customer-999": {"customer-999", "Cliente Inactivo", "inactivo@example.com", false},
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func notFound(w http.ResponseWriter, kind, id string) {
	writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " not found", "id": id})
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	})
	mux.HandleFunc("GET /products/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if p, ok := products[id]; ok {
			writeJSON(w, http.StatusOK, p)
			return
		}
		notFound(w, "product", id)
	})
	mux.HandleFunc("GET /customers/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if c, ok := customers[id]; ok {
			writeJSON(w, http.StatusOK, c)
			return
		}
		notFound(w, "customer", id)
	})
	return mux
}

func main() {
	addr := ":" + getenv("PORT", "8081")
	srv := &http.Server{
		Addr:              addr,
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("enrichment-api listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
