package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mo11y/internal/server"
	"mo11y/internal/storage"
)

func main() {
	// Configuration
	port := 4318 // Standard OTLP/HTTP port
	dbPath := os.Getenv("MO11Y_DB_PATH")
	if dbPath == "" {
		dbPath = "mo11y.duckdb"
	}

	retentionCfg := storage.CleanupConfig{
		RetentionHours:      getEnvInt("MO11Y_RETENTION_HOURS", 168),
		CleanupIntervalMins: getEnvInt("MO11Y_CLEANUP_INTERVAL_MINS", 60),
	}

	// Initialize storage
	store, err := storage.New(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	log.Printf("Connected to DuckDB: %s", dbPath)

	// Create cancellable context for cleanup worker
	ctx, cancel := context.WithCancel(context.Background())

	// Start cleanup worker
	go store.StartCleanupWorker(ctx, retentionCfg)

	// Create server
	srv := server.New(server.Config{
		Port:         port,
		RetentionCfg: retentionCfg,
	}, store)
	log.Printf("Starting server on :%d", port)

	// Start server in goroutine
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gracefully...")

	// Stop cleanup worker
	cancel()

	// Give outstanding requests 5 seconds to complete
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	if err := store.Close(); err != nil {
		log.Printf("Error closing storage: %v", err)
	}

	log.Println("Server exited")
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func init() {
	// Print startup banner
	fmt.Println(`
                 _ _       
  _ __ ___   ___| / |_   _ 
 | '_ ` + "`" + ` _ \ / _ \ | | | | |
 | | | | | | (_) | | | |_| |
 |_| |_| |_|\___/|_|_|\__, |
                      |___/`)
}
