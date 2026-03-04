package main

import (
	"fmt"
	"sync"
	"time"

	"gex-collector/internal/api"
	"gex-collector/internal/database"
	"gex-collector/internal/utils"
)

// Collector handles the poll → parse → write loop for a single ticker
type Collector struct {
	client     *api.Client
	writer     *database.DataWriter
	ticker     string
	tiers      []string
	collectAll bool
	interval   time.Duration
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

// NewCollector creates a new collector for a single ticker
func NewCollector(client *api.Client, writer *database.DataWriter, ticker string, tiers []string, collectAll bool, interval time.Duration) *Collector {
	return &Collector{
		client:     client,
		writer:     writer,
		ticker:     ticker,
		tiers:      tiers,
		collectAll: collectAll,
		interval:   interval,
		stopChan:   make(chan struct{}),
	}
}

// Start begins the collection loop
func (c *Collector) Start() {
	c.wg.Add(1)
	go c.run()
}

// Stop gracefully stops the collector
func (c *Collector) Stop() {
	close(c.stopChan)
	c.wg.Wait()
}

func (c *Collector) run() {
	defer c.wg.Done()

	utils.LogAlways("Collector started for %s (interval: %v, tiers: %v, collectAll: %v)",
		c.ticker, c.interval, c.tiers, c.collectAll)

	// Initial fetch immediately
	c.fetch()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			utils.LogAlways("Collector stopping for %s", c.ticker)
			return
		case <-ticker.C:
			c.fetch()
		}
	}
}

func (c *Collector) fetch() {
	endpoints := c.getEndpoints()
	if len(endpoints) == 0 {
		utils.Logf("No endpoints for tiers %v", c.tiers)
		return
	}

	utils.Logf("Fetching %d endpoints for %s", len(endpoints), c.ticker)

	// Fetch all endpoints and aggregate results
	aggregated := make(map[string]interface{})
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Use a semaphore to limit concurrent requests
	sem := make(chan struct{}, 16)

	for _, endpoint := range endpoints {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := c.client.FetchEndpoint(ep, c.ticker)
			if err != nil {
				// Log subscription errors at lower level (expected)
				if _, ok := err.(*api.SubscriptionError); ok {
					utils.Logf("Subscription error for %s/%s: %v", c.ticker, ep, err)
				} else {
					utils.LogAlways("Error fetching %s/%s: %v", c.ticker, ep, err)
				}
				return
			}

			mu.Lock()
			for key, value := range result {
				if key == "_response_headers" || key == "_response_time" {
					continue
				}
				aggregated[key] = value
			}
			mu.Unlock()
		}(endpoint)
	}

	wg.Wait()

	if len(aggregated) == 0 {
		utils.Logf("No data collected for %s", c.ticker)
		return
	}

	// Extract timestamp
	var timestamp float64
	if apiTs, ok := aggregated["timestamp"].(float64); ok {
		if apiTs > 1e10 {
			timestamp = apiTs / 1000.0
		} else {
			timestamp = apiTs
		}
	} else {
		timestamp = float64(time.Now().Unix())
	}

	// Write to database
	if err := c.writer.WriteDataEntry(c.ticker, timestamp, aggregated); err != nil {
		utils.LogAlways("Write error for %s: %v", c.ticker, err)
	} else {
		fieldCount := len(aggregated)
		utils.LogAlways("Collected %s: %.0f (%d fields)", c.ticker, timestamp, fieldCount)
	}
}

func (c *Collector) getEndpoints() []string {
	if c.collectAll {
		return api.GetEndpointsForTiers(c.tiers)
	}
	return api.GetChartEndpointsForTiers(c.tiers)
}

// resolveInterval returns the polling interval based on config
func resolveInterval(refreshMs int, priority int) time.Duration {
	if refreshMs > 0 {
		return time.Duration(refreshMs) * time.Millisecond
	}

	// Priority-based defaults
	switch priority {
	case 0: // High
		return 1 * time.Second
	case 1: // Medium
		return 5 * time.Second
	case 2: // Low
		return 30 * time.Second
	default:
		return 5 * time.Second
	}
}

// formatTickers returns a human-readable list of active tiers
func formatTiers(tiers []string) string {
	if len(tiers) == 0 {
		return "none"
	}
	return fmt.Sprintf("%v", tiers)
}
