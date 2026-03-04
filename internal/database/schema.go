package database

import (
	"database/sql"
	"fmt"
	"strings"
)

// SchemaManager manages database schema creation and migration
type SchemaManager struct {
	db *sql.DB
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager(db *sql.DB) *SchemaManager {
	return &SchemaManager{db: db}
}

// EnsureTable ensures the ticker_data table exists with proper schema
func (sm *SchemaManager) EnsureTable(scalarFields []string) error {
	_, err := sm.db.Exec(`
		CREATE TABLE IF NOT EXISTS ticker_data (
			timestamp REAL PRIMARY KEY,
			profiles_blob BLOB
		) WITHOUT ROWID
	`)
	if err != nil {
		return fmt.Errorf("failed to create base table: %w", err)
	}

	existingColumns, err := sm.getExistingColumns()
	if err != nil {
		return fmt.Errorf("failed to get existing columns: %w", err)
	}

	for _, field := range scalarFields {
		sanitized := SanitizeFieldName(field)
		if sanitized == "timestamp" || sanitized == "profiles_blob" {
			continue
		}
		if !existingColumns[sanitized] {
			colType := "REAL"
			_, err := sm.db.Exec(fmt.Sprintf(
				"ALTER TABLE ticker_data ADD COLUMN %s %s",
				sanitized, colType,
			))
			if err != nil {
				if !strings.Contains(err.Error(), "duplicate column") {
					return fmt.Errorf("failed to add column %s: %w", sanitized, err)
				}
			}
		}
	}

	_, err = sm.db.Exec(`CREATE INDEX IF NOT EXISTS idx_timestamp_desc ON ticker_data(timestamp DESC)`)
	if err != nil {
		return fmt.Errorf("failed to create descending index: %w", err)
	}

	_, err = sm.db.Exec(`CREATE INDEX IF NOT EXISTS idx_timestamp_asc ON ticker_data(timestamp ASC)`)
	if err != nil {
		return fmt.Errorf("failed to create ascending index: %w", err)
	}

	return nil
}

func (sm *SchemaManager) getExistingColumns() (map[string]bool, error) {
	rows, err := sm.db.Query(`SELECT name FROM pragma_table_info('ticker_data')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns[name] = true
	}

	return columns, rows.Err()
}

// SanitizeFieldName sanitizes a field name for use as a SQL column name
func SanitizeFieldName(field string) string {
	result := strings.Builder{}
	for _, r := range field {
		if r == '-' || r == '.' || r == ' ' {
			result.WriteRune('_')
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
		}
	}

	sanitized := result.String()
	sanitized = strings.Trim(sanitized, "_")

	if len(sanitized) > 0 {
		first := sanitized[0]
		if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
			sanitized = "_" + sanitized
		}
	}

	return sanitized
}
