package logging

import (
	"io"
	"log"
	"os"
	"sync"
)

// syncWriter wraps an *os.File and ensures each Write is followed by an fsync
// to provide durable writes to disk.
type syncWriter struct {
	mu sync.Mutex
	f  *os.File
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.f.Write(p)
	if err != nil {
		return n, err
	}
	if err := w.f.Sync(); err != nil {
		return n, err
	}
	return n, nil
}

// NewLogger creates a logger that writes to stdout and to a log file
// with durable writes. It returns the logger, a closer (to flush/close the file),
// and any error encountered while opening the file.
func NewLogger(path string) (*log.Logger, func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}

	sw := &syncWriter{f: f}
	mw := io.MultiWriter(os.Stdout, sw)
	logger := log.New(mw, "[dida] ", log.Ldate|log.Ltime|log.Lmicroseconds)

	closer := func() error {
		// Ensure any buffered data is flushed and file is closed.
		if err := f.Sync(); err != nil {
			// attempt to still close even if sync fails
			_ = f.Close()
			return err
		}
		return f.Close()
	}

	return logger, closer, nil
}
