package config

// HTTP Connection Pool Configuration
const (
	HTTPPoolConnections = 32
	HTTPPoolMaxSize     = 32
)

// Timestamp Deduplication Tolerance (in seconds)
const (
	TimestampDedupToleranceDataLoading = 0.1 // 100ms tolerance
)

// Database Connection Pool Configuration
const (
	DBConnectionPoolMaxSize = 10
)

// File Write Batching Configuration
const (
	FileWriteIntervalCollectionSec    = 2.0
	FileWriteCountThresholdCollection = 5
)

// API Configuration
const (
	APIBaseURL = "https://api.gexbot.com"
)

// SQLite Connection Configuration
const (
	SQLiteConnectionIdleTimeoutSeconds     = 10.0
	SQLiteConnectionCleanupIntervalSeconds = 5.0
)

// Tier Configuration
var TierNames = []string{"classic", "state", "orderflow"}
