package database

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ConnectionPool manages database connections with idle timeout
type ConnectionPool struct {
	mu              sync.RWMutex
	connections     map[string]*pooledConnection
	maxSize         int
	idleTimeout     time.Duration
	cleanupInterval time.Duration
	cleanupTimer    *time.Timer
	stopCleanup     chan struct{}
}

type pooledConnection struct {
	db       *sql.DB
	lastUsed time.Time
	filepath string
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxSize int, idleTimeout, cleanupInterval time.Duration) *ConnectionPool {
	pool := &ConnectionPool{
		connections:     make(map[string]*pooledConnection),
		maxSize:         maxSize,
		idleTimeout:     idleTimeout,
		cleanupInterval: cleanupInterval,
		stopCleanup:     make(chan struct{}),
	}
	pool.startCleanup()
	return pool
}

// GetConnection gets or creates a database connection
func (p *ConnectionPool) GetConnection(filepath string, readOnly bool) (*sql.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pc, exists := p.connections[filepath]; exists {
		if err := pc.db.Ping(); err == nil {
			pc.lastUsed = time.Now()
			return pc.db, nil
		}
		pc.db.Close()
		delete(p.connections, filepath)
	}

	var db *sql.DB
	var err error

	if readOnly {
		db, err = sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", filepath))
	} else {
		db, err = sql.Open("sqlite", filepath)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := p.configureConnection(db, readOnly); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure connection: %w", err)
	}

	p.connections[filepath] = &pooledConnection{
		db:       db,
		lastUsed: time.Now(),
		filepath: filepath,
	}

	return db, nil
}

func (p *ConnectionPool) configureConnection(db *sql.DB, readOnly bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA cache_size=-20000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=268435456",
	}

	for _, pragma := range pragmas {
		conn.ExecContext(ctx, pragma)
	}

	if readOnly {
		conn.ExecContext(ctx, "PRAGMA query_only=1")
		conn.ExecContext(ctx, "PRAGMA read_uncommitted=1")
		conn.ExecContext(ctx, "PRAGMA busy_timeout=10000")
	} else {
		conn.ExecContext(ctx, "PRAGMA page_size=8192")
	}

	return nil
}

func (p *ConnectionPool) startCleanup() {
	p.cleanupTimer = time.NewTimer(p.cleanupInterval)
	go func() {
		for {
			select {
			case <-p.cleanupTimer.C:
				p.cleanupIdleConnections()
				p.cleanupTimer.Reset(p.cleanupInterval)
			case <-p.stopCleanup:
				return
			}
		}
	}()
}

func (p *ConnectionPool) cleanupIdleConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	for fp, pc := range p.connections {
		if now.Sub(pc.lastUsed) > p.idleTimeout {
			pc.db.Close()
			delete(p.connections, fp)
		}
	}
}

// Close closes all connections with WAL checkpoint
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.stopCleanup)
	if p.cleanupTimer != nil {
		p.cleanupTimer.Stop()
	}

	for _, pc := range p.connections {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := pc.db.Conn(ctx)
		if err == nil {
			conn.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
			conn.Close()
		}
		cancel()
		pc.db.Close()
	}

	p.connections = make(map[string]*pooledConnection)
	return nil
}
