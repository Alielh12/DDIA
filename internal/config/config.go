package config

import "fmt"

// Config holds server configuration values.
// Values may come from flags or environment variables.
type Config struct {
	DataDir string
	LogFile string
	Addr    string
}

// Validate ensures required fields are set and sensible.
func (c Config) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("DataDir is required")
	}
	if c.LogFile == "" {
		return fmt.Errorf("LogFile is required")
	}
	if c.Addr == "" {
		return fmt.Errorf("Addr is required")
	}
	return nil
}
