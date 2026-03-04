package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"gex-collector/internal/config"
	"gex-collector/internal/utils"
)

// Client handles HTTP requests to the GEXBot API
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	mu         sync.RWMutex
}

// NewClient creates a new API client with connection pooling
func NewClient(apiKey string) *Client {
	transport := &http.Transport{
		MaxIdleConns:        config.HTTPPoolConnections,
		MaxIdleConnsPerHost: config.HTTPPoolMaxSize,
		IdleConnTimeout:     90 * time.Second,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	return &Client{
		apiKey:     apiKey,
		baseURL:    config.APIBaseURL,
		httpClient: httpClient,
	}
}

// FetchEndpoint fetches data from a specific API endpoint
func (c *Client) FetchEndpoint(endpoint, ticker string) (map[string]interface{}, error) {
	urlTemplate, ok := Endpoints[endpoint]
	if !ok {
		return nil, fmt.Errorf("unknown endpoint: %s", endpoint)
	}

	url := fmt.Sprintf(urlTemplate, c.baseURL, ticker, c.apiKey)

	maxRetries := 3
	retryDelays := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		utils.Logf("API: Fetching %s for %s (attempt %d/%d)", endpoint, ticker, attempt+1, maxRetries)

		resp, err := c.httpClient.Get(url)
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				time.Sleep(retryDelays[attempt])
				continue
			}
			return nil, fmt.Errorf("request error after %d attempts: %w", maxRetries, err)
		}

		if resp.StatusCode == 401 {
			resp.Body.Close()
			return nil, &SubscriptionError{
				Endpoint: endpoint,
				Message:  fmt.Sprintf("Unauthorized access to %s for %s", endpoint, ticker),
			}
		} else if resp.StatusCode == 403 {
			resp.Body.Close()
			return nil, &SubscriptionError{
				Endpoint: endpoint,
				Message:  fmt.Sprintf("Access forbidden to %s for %s", endpoint, ticker),
			}
		} else if resp.StatusCode == 429 {
			retryAfter := resp.Header.Get("Retry-After")
			resp.Body.Close()
			return nil, &RateLimitError{
				Endpoint:   endpoint,
				Message:    fmt.Sprintf("Rate limit exceeded for %s on %s", endpoint, ticker),
				RetryAfter: retryAfter,
			}
		} else if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			bodyStr := string(body)
			if len(bodyStr) > 200 {
				bodyStr = bodyStr[:200]
			}
			return nil, &RequestError{
				Endpoint:   endpoint,
				StatusCode: resp.StatusCode,
				Message:    fmt.Sprintf("HTTP %d error fetching %s for %s: %s", resp.StatusCode, endpoint, ticker, bodyStr),
			}
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				time.Sleep(retryDelays[attempt])
				continue
			}
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, &RequestError{
				Endpoint:      endpoint,
				Message:       fmt.Sprintf("Invalid JSON response from %s for %s: %v", endpoint, ticker, err),
				OriginalError: err,
			}
		}

		utils.Logf("API: Successfully fetched %s for %s (fields: %d)", endpoint, ticker, len(data))
		return data, nil
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// Close closes the HTTP client
func (c *Client) Close() {
	// HTTP client connections close on their own
}
