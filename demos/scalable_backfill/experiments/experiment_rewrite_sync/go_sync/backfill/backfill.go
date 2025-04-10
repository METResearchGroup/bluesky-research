package backfill

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds configuration for the backfill process
type Config struct {
	Concurrency int
	BatchSize   int
	Timeout     time.Duration
	RateLimit   int
	RateWindow  int
	StartDate   string
	EndDate     string
	OutputFile  string
	Logger      *log.Logger
}

// Result contains statistics about the backfill run
type Result struct {
	ProcessedCount   int
	SuccessCount     int
	FailureCount     int
	SuccessRate      float64
	RecordCount      int
	PeakMemoryMB     float64
	ApiCalls         int
	RateLimitedCalls int
}

// PLCDocument represents the PLC directory document structure
type PLCDocument struct {
	ID      string `json:"id"`
	Service []struct {
		ID              string `json:"id"`
		ServiceEndpoint string `json:"serviceEndpoint"`
		Type            string `json:"type"`
	} `json:"service"`
	AlsoKnownAs []string `json:"alsoKnownAs"`
}

// RateLimiter manages API request rate limits
type RateLimiter struct {
	requests      []time.Time
	limit         int
	windowSeconds int
	mu            sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(limit, windowSeconds int) *RateLimiter {
	return &RateLimiter{
		requests:      make([]time.Time, 0, limit),
		limit:         limit,
		windowSeconds: windowSeconds,
	}
}

// Wait blocks until a request can be made, returning true if allowed or false if the context is done
func (rl *RateLimiter) Wait() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-time.Duration(rl.windowSeconds) * time.Second)

	// Remove requests outside the window
	validRequests := 0
	for _, t := range rl.requests {
		if t.After(windowStart) {
			validRequests++
		}
	}

	// Check if we're at the limit
	if validRequests >= rl.limit {
		// Calculate sleep time - when the oldest request will drop out of the window
		if len(rl.requests) > 0 {
			oldestValidRequest := rl.requests[len(rl.requests)-validRequests]
			sleepTime := oldestValidRequest.Add(time.Duration(rl.windowSeconds) * time.Second).Sub(now)

			rl.mu.Unlock()
			time.Sleep(sleepTime + 10*time.Millisecond) // Add a small buffer
			rl.mu.Lock()
		}
	}

	// Add this request to the queue
	rl.requests = append(rl.requests, time.Now())

	// Keep only the most recent requests to prevent unbounded growth
	if len(rl.requests) > rl.limit*2 {
		rl.requests = rl.requests[len(rl.requests)-rl.limit:]
	}

	return true
}

// Run executes the backfill process for the given DIDs
func Run(dids []string, config Config) (*Result, error) {
	if config.Logger == nil {
		config.Logger = log.New(io.Discard, "", 0)
	}

	logger := config.Logger
	logger.Printf("Starting backfill for %d DIDs with concurrency %d", len(dids), config.Concurrency)

	// Set up HTTP client with connection pooling
	client := &http.Client{
		Timeout: config.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        config.Concurrency,
			MaxIdleConnsPerHost: config.Concurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Initialize rate limiter
	rateLimiter := NewRateLimiter(config.RateLimit, config.RateWindow)

	// Create channels for work distribution and collection
	work := make(chan string, config.Concurrency*2)
	results := make(chan struct {
		did     string
		success bool
		records int
		err     error
	}, config.Concurrency*2)

	// Set up counters
	var processedCount int32
	var successCount int32
	var failureCount int32
	var recordCount int32
	var apiCallCount int32
	var rateLimitedCount int32
	var peakMemoryMB float64
	var memMutex sync.Mutex

	// Start memory monitoring
	stopMemMonitor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		var mem runtime.MemStats
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&mem)
				memoryMB := float64(mem.Alloc) / 1024 / 1024
				memMutex.Lock()
				if memoryMB > peakMemoryMB {
					peakMemoryMB = memoryMB
				}
				memMutex.Unlock()
			case <-stopMemMonitor:
				return
			}
		}
	}()

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for did := range work {
				// Wait for rate limit if needed
				if rateLimiter.Wait() {
					// Process the DID
					success, count, err := processDID(did, client, config, &apiCallCount, &rateLimitedCount)
					results <- struct {
						did     string
						success bool
						records int
						err     error
					}{did, success, count, err}
				}
			}
		}(i)
	}

	// Start a goroutine to close the results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Start a goroutine to distribute work
	go func() {
		for _, did := range dids {
			work <- did
		}
		close(work)
	}()

	// Process results as they come in
	for result := range results {
		atomic.AddInt32(&processedCount, 1)
		processed := atomic.LoadInt32(&processedCount)

		if processed%100 == 0 || processed == int32(len(dids)) {
			logger.Printf("Processed %d/%d DIDs (%.1f%%)",
				processed, len(dids), float64(processed)/float64(len(dids))*100)
		}

		if result.success {
			atomic.AddInt32(&successCount, 1)
			atomic.AddInt32(&recordCount, int32(result.records))
		} else {
			atomic.AddInt32(&failureCount, 1)
			if result.err != nil {
				logger.Printf("Error processing DID %s: %v", result.did, result.err)
			}
		}
	}

	// Stop memory monitoring
	close(stopMemMonitor)

	// Calculate final stats
	successRate := float64(successCount) / float64(processedCount)
	if processedCount == 0 {
		successRate = 0
	}

	logger.Printf("Backfill complete: %d processed, %d successful, %d failed",
		processedCount, successCount, failureCount)
	logger.Printf("API calls: %d, Rate limited: %d, Peak memory: %.2f MB",
		apiCallCount, rateLimitedCount, peakMemoryMB)

	return &Result{
		ProcessedCount:   int(processedCount),
		SuccessCount:     int(successCount),
		FailureCount:     int(failureCount),
		SuccessRate:      successRate,
		RecordCount:      int(recordCount),
		PeakMemoryMB:     peakMemoryMB,
		ApiCalls:         int(apiCallCount),
		RateLimitedCalls: int(rateLimitedCount),
	}, nil
}

// processDID handles fetching and processing data for a single DID
func processDID(did string, client *http.Client, config Config, apiCalls, rateLimitedCalls *int32) (bool, int, error) {
	// Get PLC document
	plcDoc, err := getPlcDirectory(did, client, apiCalls, rateLimitedCalls)
	if err != nil {
		return false, 0, fmt.Errorf("failed to get PLC directory: %w", err)
	}

	// Check if we have service information
	if len(plcDoc.Service) == 0 {
		return false, 0, fmt.Errorf("no service information in PLC document")
	}

	// Extract PDS endpoint
	pdsEndpoint := plcDoc.Service[0].ServiceEndpoint

	// Get Bluesky handle from alsoKnownAs - will be used for metadata
	_ = func() string {
		for _, aka := range plcDoc.AlsoKnownAs {
			if len(aka) > 5 && aka[:5] == "at://" {
				return aka[5:]
			}
		}
		return ""
	}()

	// Get records
	records, err := getBskyRecords(did, pdsEndpoint, client, apiCalls, rateLimitedCalls)
	if err != nil {
		return false, 0, fmt.Errorf("failed to get Bluesky records: %w", err)
	}

	return true, len(records), nil
}

// getPlcDirectory fetches the PLC directory document for a DID
func getPlcDirectory(did string, client *http.Client, apiCalls, rateLimitedCalls *int32) (*PLCDocument, error) {
	url := fmt.Sprintf("https://plc.directory/%s", did)

	atomic.AddInt32(apiCalls, 1)
	resp, err := client.Get(url)
	if err != nil {
		if err.Error() == "rate limit exceeded" {
			atomic.AddInt32(rateLimitedCalls, 1)
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var plcDoc PLCDocument
	if err := json.NewDecoder(resp.Body).Decode(&plcDoc); err != nil {
		return nil, err
	}

	return &plcDoc, nil
}

// getBskyRecords fetches user records from their PDS
func getBskyRecords(did string, pdsEndpoint string, client *http.Client, apiCalls, rateLimitedCalls *int32) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("%s/xrpc/com.atproto.sync.getRepo?did=%s", pdsEndpoint, did)

	atomic.AddInt32(apiCalls, 1)
	resp, err := client.Get(url)
	if err != nil {
		if err.Error() == "rate limit exceeded" {
			atomic.AddInt32(rateLimitedCalls, 1)
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// In a real implementation, we'd parse the CAR file here
	// For this experiment, we'll simulate parsing by just counting the content size
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Simulate extracting records - in reality, we'd parse the CAR file properly
	recordCount := len(body) / 5000 // Just a rough approximation for simulation
	if recordCount < 1 {
		recordCount = 1
	}

	mockRecords := make([]map[string]interface{}, recordCount)
	for i := 0; i < recordCount; i++ {
		mockRecords[i] = map[string]interface{}{
			"id":        fmt.Sprintf("record_%d", i),
			"createdAt": time.Now().Format(time.RFC3339),
		}
	}

	return mockRecords, nil
}
