package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/dida/internal/config"
	"github.com/example/dida/internal/logging"
	"github.com/example/dida/internal/server"
	"github.com/example/dida/internal/storage"
)

func main() {
	// CLI flags (override environment vars)
	dataDir := flag.String("data-dir", "", "path to data directory (env DIDA_DATA_DIR)")
	logFile := flag.String("log-file", "", "path to log file (env DIDA_LOG_FILE)")
	addr := flag.String("addr", "", "server listen address (env DIDA_ADDR, default :8080)")
	flag.Parse()

	// Load configuration
	cfg := config.Config{
		DataDir: firstNonEmpty(*dataDir, os.Getenv("DIDA_DATA_DIR"), "./data"),
		LogFile: firstNonEmpty(*logFile, os.Getenv("DIDA_LOG_FILE"), "./dida.log"),
		Addr:    firstNonEmpty(*addr, os.Getenv("DIDA_ADDR"), ":8080"),
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create data dir %q: %v\n", cfg.DataDir, err)
		os.Exit(1)
	}

	// Setup durable logger
	logger, closer, err := logging.NewLogger(cfg.LogFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	// Ensure logger is closed and flushed on exit
	defer func() {
		if err := closer(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close log file: %v\n", err)
		}
	}()

	logger.Printf("starting server; data_dir=%s addr=%s log_file=%s", cfg.DataDir, cfg.Addr, cfg.LogFile)

	// Initialize storage
	st, err := storage.New(cfg.DataDir)
	if err != nil {
		logger.Fatalf("storage init: %v", err)
	}

	// Initialize and start HTTP server
	srv, err := server.New(cfg, logger, st)
	if err != nil {
		logger.Fatalf("server init: %v", err)
	}

	// Graceful shutdown on signals
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run server in background
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("server run failed: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-stopCtx.Done()
	logger.Println("shutdown signal received; stopping server")

	// Allow up to 10s for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Printf("error during shutdown: %v", err)
	}

	logger.Println("server stopped")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
