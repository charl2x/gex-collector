package database

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gex-collector/internal/config"
	"gex-collector/internal/utils"
)

// DataWriter handles writing market data to SQLite databases
type DataWriter struct {
	pool             *ConnectionPool
	mu               sync.RWMutex
	pendingWrites    map[string][]*PendingWrite
	firstPendingTime map[string]time.Time
	lastFlushTime    map[string]time.Time
	dataDir          string

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// PendingWrite represents a pending database write
type PendingWrite struct {
	Ticker    string
	Timestamp float64
	Scalars   map[string]interface{}
	Profiles  map[string]interface{}
	Date      time.Time
}

// NewDataWriter creates a new data writer
func NewDataWriter(dataDir string) *DataWriter {
	pool := NewConnectionPool(
		config.DBConnectionPoolMaxSize,
		time.Duration(config.SQLiteConnectionIdleTimeoutSeconds)*time.Second,
		time.Duration(config.SQLiteConnectionCleanupIntervalSeconds)*time.Second,
	)

	dw := &DataWriter{
		pool:             pool,
		pendingWrites:    make(map[string][]*PendingWrite),
		firstPendingTime: make(map[string]time.Time),
		lastFlushTime:    make(map[string]time.Time),
		dataDir:          dataDir,
		stopChan:         make(chan struct{}),
	}

	dw.startBackgroundFlusher()
	return dw
}

func (dw *DataWriter) startBackgroundFlusher() {
	dw.wg.Add(1)
	go func() {
		defer dw.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-dw.stopChan:
				return
			case <-ticker.C:
				dw.checkAndFlushPending()
			}
		}
	}()
}

func (dw *DataWriter) checkAndFlushPending() {
	dw.mu.RLock()
	tickersToCheck := make([]string, 0)
	for ticker, pending := range dw.pendingWrites {
		if len(pending) > 0 {
			tickersToCheck = append(tickersToCheck, ticker)
		}
	}
	dw.mu.RUnlock()

	for _, ticker := range tickersToCheck {
		if dw.shouldFlush(ticker) {
			if err := dw.FlushTicker(ticker); err != nil {
				utils.Logf("Background flusher: flush failed for %s: %v", ticker, err)
			}
		}
	}
}

func (dw *DataWriter) shouldFlush(ticker string) bool {
	dw.mu.RLock()
	defer dw.mu.RUnlock()

	pending := dw.pendingWrites[ticker]
	if len(pending) == 0 {
		return false
	}

	if len(pending) >= config.FileWriteCountThresholdCollection {
		return true
	}

	firstPending, exists := dw.firstPendingTime[ticker]
	if !exists {
		return true
	}

	return time.Since(firstPending) >= time.Duration(config.FileWriteIntervalCollectionSec*float64(time.Second))
}

// WriteDataEntry writes a single data entry (queues for batch write)
func (dw *DataWriter) WriteDataEntry(ticker string, timestamp float64, data map[string]interface{}) error {
	dw.mu.Lock()

	scalars := make(map[string]interface{})
	profiles := make(map[string]interface{})

	if prof, ok := data["profiles"].(map[string]interface{}); ok {
		profiles = prof
	}

	for key, value := range data {
		if key == "profiles" || key == "timestamp" || key == "ticker" || key == "_response_headers" || key == "_response_time" {
			continue
		}

		switch v := value.(type) {
		case []interface{}, map[string]interface{}:
			profiles[key] = v
		default:
			if v == nil || v == 0 || v == 0.0 || v == "" || v == false {
				continue
			}
			scalars[key] = v
		}
	}

	// Use current market date for directory
	currentMarketDate := utils.GetMarketDate()
	dateOnly := time.Date(currentMarketDate.Year(), currentMarketDate.Month(), currentMarketDate.Day(), 0, 0, 0, 0, utils.GetMarketTimezone())

	var entryDate time.Time
	if utils.IsWeekend(dateOnly) {
		entryDate = utils.GetLastTradingDay(dateOnly)
	} else {
		entryDate = dateOnly
	}

	if dw.pendingWrites[ticker] == nil {
		dw.pendingWrites[ticker] = make([]*PendingWrite, 0)
	}

	dw.pendingWrites[ticker] = append(dw.pendingWrites[ticker], &PendingWrite{
		Ticker:    ticker,
		Timestamp: timestamp,
		Scalars:   scalars,
		Profiles:  profiles,
		Date:      entryDate,
	})

	pendingCount := len(dw.pendingWrites[ticker])
	_, hasFlushHistory := dw.lastFlushTime[ticker]

	if pendingCount == 1 {
		dw.firstPendingTime[ticker] = time.Now()
	}

	dw.mu.Unlock()

	// Force flush on first write to create DB file
	if !hasFlushHistory {
		go func() {
			if err := dw.FlushTicker(ticker); err != nil {
				utils.Logf("First flush failed for %s: %v", ticker, err)
			}
		}()
	}

	return nil
}

// FlushTicker flushes all pending writes for a ticker
func (dw *DataWriter) FlushTicker(ticker string) error {
	dw.mu.Lock()
	pending := dw.pendingWrites[ticker]
	if len(pending) == 0 {
		dw.mu.Unlock()
		return nil
	}

	dw.pendingWrites[ticker] = make([]*PendingWrite, 0)
	delete(dw.firstPendingTime, ticker)
	dw.lastFlushTime[ticker] = time.Now()
	dw.mu.Unlock()

	byDate := make(map[time.Time][]*PendingWrite)
	for _, write := range pending {
		date := time.Date(write.Date.Year(), write.Date.Month(), write.Date.Day(), 0, 0, 0, 0, time.UTC)
		byDate[date] = append(byDate[date], write)
	}

	for date, writes := range byDate {
		if err := dw.flushDate(ticker, date, writes); err != nil {
			dw.mu.Lock()
			dw.pendingWrites[ticker] = append(dw.pendingWrites[ticker], writes...)
			dw.mu.Unlock()
			return err
		}
	}

	return nil
}

func (dw *DataWriter) flushDate(ticker string, date time.Time, writes []*PendingWrite) error {
	// Deduplicate
	writes = dw.deduplicateWrites(writes, config.TimestampDedupToleranceDataLoading)

	dbPath := dw.getDBPath(ticker, date)
	utils.Logf("Flushing %d writes for %s to %s", len(writes), ticker, dbPath)

	db, err := dw.pool.GetConnection(dbPath, false)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	// Collect all scalar fields
	scalarFields := make([]string, 0)
	scalarFieldsSet := make(map[string]bool)
	for _, write := range writes {
		for field := range write.Scalars {
			if !scalarFieldsSet[field] {
				scalarFields = append(scalarFields, field)
				scalarFieldsSet[field] = true
			}
		}
	}

	// Pre-create expected chart columns
	for _, col := range []string{"spot", "zero_gamma", "major_pos_vol", "major_neg_vol",
		"major_long_gamma", "major_short_gamma", "major_positive", "major_negative",
		"major_pos_oi", "major_neg_oi", "net_gex_vol"} {
		if !scalarFieldsSet[col] {
			scalarFields = append(scalarFields, col)
			scalarFieldsSet[col] = true
		}
	}

	schemaManager := NewSchemaManager(db)
	if err := schemaManager.EnsureTable(scalarFields); err != nil {
		return fmt.Errorf("failed to ensure schema: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	allScalarFields := make(map[string]bool)
	for _, write := range writes {
		for field := range write.Scalars {
			allScalarFields[field] = true
		}
	}

	scalarFieldsList := make([]string, 0, len(allScalarFields))
	for field := range allScalarFields {
		scalarFieldsList = append(scalarFieldsList, field)
	}

	insertSQL := dw.buildInsertStatement(scalarFieldsList)
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, write := range writes {
		var profilesBlob []byte
		if len(write.Profiles) > 0 {
			profilesJSON, err := json.Marshal(write.Profiles)
			if err != nil {
				return fmt.Errorf("failed to marshal profiles: %w", err)
			}
			var buf bytes.Buffer
			gz := gzip.NewWriter(&buf)
			if _, err := gz.Write(profilesJSON); err != nil {
				return fmt.Errorf("failed to compress profiles: %w", err)
			}
			if err := gz.Close(); err != nil {
				return fmt.Errorf("failed to close gzip writer: %w", err)
			}
			profilesBlob = buf.Bytes()
		}

		args := []interface{}{write.Timestamp, profilesBlob}
		for _, field := range scalarFieldsList {
			if value, ok := write.Scalars[field]; ok {
				args = append(args, value)
			} else {
				args = append(args, nil)
			}
		}

		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("failed to insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// WAL checkpoint
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err == nil {
		conn.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
		conn.Close()
	}

	utils.Logf("Successfully flushed %d writes for %s", len(writes), ticker)
	return nil
}

func (dw *DataWriter) buildInsertStatement(scalarFields []string) string {
	columns := []string{"timestamp", "profiles_blob"}
	placeholders := []string{"?", "?"}

	seen := make(map[string]bool)
	seen["timestamp"] = true
	seen["profiles_blob"] = true

	for _, field := range scalarFields {
		sanitized := SanitizeFieldName(field)
		if !seen[sanitized] {
			columns = append(columns, sanitized)
			placeholders = append(placeholders, "?")
			seen[sanitized] = true
		}
	}

	return fmt.Sprintf(
		"INSERT OR REPLACE INTO ticker_data (%s) VALUES (%s)",
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
}

func (dw *DataWriter) getDBPath(ticker string, date time.Time) string {
	var marketDate time.Time
	if utils.IsWeekend(date) {
		marketDate = utils.GetLastTradingDay(date)
	} else {
		marketDate = date
	}

	dateStr := marketDate.Format("01.02.2006")
	dir := fmt.Sprintf("%s %s", dw.dataDir, dateStr)

	if err := os.MkdirAll(dir, 0755); err != nil {
		utils.Logf("WARNING: Failed to create directory %s: %v", dir, err)
	}

	return filepath.Join(dir, fmt.Sprintf("%s.db", ticker))
}

func (dw *DataWriter) deduplicateWrites(writes []*PendingWrite, tolerance float64) []*PendingWrite {
	if len(writes) == 0 {
		return writes
	}

	sorted := make([]*PendingWrite, len(writes))
	copy(sorted, writes)

	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i].Timestamp > sorted[j].Timestamp {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	result := make([]*PendingWrite, 0)
	for i := 0; i < len(sorted); i++ {
		if i < len(sorted)-1 {
			timeDiff := sorted[i+1].Timestamp - sorted[i].Timestamp
			if timeDiff <= tolerance {
				continue
			}
		}
		result = append(result, sorted[i])
	}

	return result
}

// Close flushes pending writes and closes all connections
func (dw *DataWriter) Close() error {
	close(dw.stopChan)
	dw.wg.Wait()

	dw.mu.Lock()
	tickersToFlush := make([]string, 0)
	for ticker := range dw.pendingWrites {
		if len(dw.pendingWrites[ticker]) > 0 {
			tickersToFlush = append(tickersToFlush, ticker)
		}
	}
	dw.mu.Unlock()

	for _, ticker := range tickersToFlush {
		if err := dw.FlushTicker(ticker); err != nil {
			utils.Logf("Warning: failed to flush %s on close: %v", ticker, err)
		}
	}

	return dw.pool.Close()
}
