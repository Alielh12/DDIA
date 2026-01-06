package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/example/dida/internal/config"
	"github.com/example/dida/internal/storage"
)

// Server wraps the HTTP server and any server-scoped resources.
type Server struct {
	httpServer         *http.Server
	logger             *log.Logger
	store              *storage.Storage
	replicationOffsets map[string]int64
	httpClient         *http.Client
}

// New constructs a new Server with routes registered.
func New(cfg config.Config, logger *log.Logger, store *storage.Storage) (*Server, error) {
	mux := http.NewServeMux()
	s := &Server{logger: logger, store: store, replicationOffsets: make(map[string]int64), httpClient: &http.Client{Timeout: 10 * time.Second}}

	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/kv/", s.kvHandler)
	mux.HandleFunc("/internal/replicate", s.internalReplicateHandler)
	mux.HandleFunc("/internal/version", s.internalVersionHandler)
	mux.HandleFunc("/metrics", s.metricsHandler)
	mux.HandleFunc("/admin/replicate", s.adminReplicateHandler) // trigger push to follower

	h := &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.httpServer = h
	logger.Printf("routes registered: /health, /kv/{key}, /internal/replicate, /admin/replicate")
	return s, nil
}

// Start runs the HTTP server. It blocks until ListenAndServe returns.
func (s *Server) Start() error {
	s.logger.Printf("listening on %s", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the server using the provided context.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Println("shutting down HTTP server")
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}
	// close storage
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			s.logger.Printf("error closing storage: %v", err)
		}
	}
	return nil
}

// healthHandler responds with a small JSON payload to indicate liveness.
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte(`{"status":"ok"}`))
	if err != nil {
		s.logger.Printf("failed to write health response: %v", err)
	}
}

// kvHandler handles both GET and PUT for /kv/{key}
func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	if strings.ContainsAny(key, ",\n\r") {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		// Limit body to 1MB
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
			return
		}
		v, err := s.store.Append(key, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("append: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]int64{"version": v})
	case http.MethodGet:
		val, ver, err := s.store.Read(key)
		if err != nil {
			if err == storage.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, fmt.Sprintf("read: %v", err), http.StatusInternalServerError)
			return
		}
		// return base64-encoded value and version
		enc := base64.RawStdEncoding.EncodeToString(val)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"value": enc, "version": ver})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// internalReplicateHandler accepts replication batches from a leader.
func (s *Server) internalReplicateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Records []string `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid body: %v", err), http.StatusBadRequest)
		return
	}
	if err := s.store.ApplyReplication(req.Records); err != nil {
		http.Error(w, fmt.Sprintf("apply replicate: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// internalVersionHandler reports the current applied version (for replicas).
func (s *Server) internalVersionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ver := s.store.AppliedVersion()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]int64{"applied_version": ver})
}

// metricsHandler returns a JSON snapshot of in-memory metrics.
func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ms := s.store.MetricsSnapshot()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ms)
}

// adminReplicateHandler is an admin endpoint that triggers pushing the WAL
// tail to a follower URL. Query params: target (required), maxbytes (opt),
// delayms (opt) — artificial delay to simulate lag.
func (s *Server) adminReplicateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	target := q.Get("target")
	if target == "" {
		http.Error(w, "target required", http.StatusBadRequest)
		return
	}
	if _, err := url.ParseRequestURI(target); err != nil {
		http.Error(w, fmt.Sprintf("invalid target url: %v", err), http.StatusBadRequest)
		return
	}
	maxBytes := 0
	if mb := q.Get("maxbytes"); mb != "" {
		if n, err := strconv.Atoi(mb); err == nil && n > 0 {
			maxBytes = n
		}
	}
	delay := 0
	if d := q.Get("delayms"); d != "" {
		if n, err := strconv.Atoi(d); err == nil && n >= 0 {
			delay = n
		}
	}

	// get current offset for target
	off := int64(0)
	s.logger.Printf("replicate: target=%s maxbytes=%d delayms=%d", target, maxBytes, delay)
	if v, ok := s.replicationOffsets[target]; ok {
		off = v
	}
	// fetch batch from storage
	recs, newOff, err := s.store.ReplicationBatch(off, maxBytes)
	if err != nil {
		http.Error(w, fmt.Sprintf("replication batch: %v", err), http.StatusInternalServerError)
		return
	}
	if len(recs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// simulate delay
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	// send to follower
	body := struct {
		Records []string `json:"records"`
	}{Records: recs}
	b, _ := json.Marshal(body)
	reqURL := strings.TrimRight(target, "/") + "/internal/replicate"
	req2, err := http.NewRequestWithContext(r.Context(), http.MethodPost, reqURL, strings.NewReader(string(b)))
	if err != nil {
		http.Error(w, fmt.Sprintf("build request: %v", err), http.StatusInternalServerError)
		return
	}
	req2.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req2)
	if err != nil {
		http.Error(w, fmt.Sprintf("send replicate: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		http.Error(w, fmt.Sprintf("replicate failed: status=%d body=%s", resp.StatusCode, string(b)), http.StatusBadGateway)
		return
	}
	// success: update offset
	s.replicationOffsets[target] = newOff
	w.WriteHeader(http.StatusOK)
}
