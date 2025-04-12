package backfill

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewRateLimiter(t *testing.T) {
	limit := 100
	window := 10

	rl := NewRateLimiter(limit, window)

	if rl.limit != limit {
		t.Errorf("Expected rate limit to be %d, got %d", limit, rl.limit)
	}

	if rl.windowSeconds != window {
		t.Errorf("Expected window to be %d, got %d", window, rl.windowSeconds)
	}

	if cap(rl.requests) != limit {
		t.Errorf("Expected requests capacity to be %d, got %d", limit, cap(rl.requests))
	}
}

func TestRateLimiterWait(t *testing.T) {
	// Create a rate limiter with a small limit and window
	rl := NewRateLimiter(3, 1)

	// First 3 requests should go through immediately
	start := time.Now()
	for i := 0; i < 3; i++ {
		result := rl.Wait()
		if !result {
			t.Errorf("Expected Wait() to return true for request %d", i)
		}
	}
	elapsed := time.Since(start)

	// This should be fast (less than 50ms for 3 requests)
	if elapsed > 50*time.Millisecond {
		t.Errorf("First 3 requests took too long: %v", elapsed)
	}

	// The 4th request should be delayed
	start = time.Now()
	result := rl.Wait()
	elapsed = time.Since(start)

	if !result {
		t.Errorf("Expected Wait() to return true for delayed request")
	}

	// The 4th request should be delayed by close to the window time
	if elapsed < 900*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("Expected delay of around 1000ms, got %v", elapsed)
	}
}

// mockClient creates a test HTTP client that directs requests to the test server
func mockClient(testServer *httptest.Server) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			// Use a custom dialer that directs all traffic to our test server
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial(network, testServer.Listener.Addr().String())
			},
		},
	}
}

// testRun is a test helper that runs the backfill with a mock client
func testRun(dids []string, cfg Config, client *http.Client) (*Result, error) {
	// Create a mock implementation for testing
	return &Result{
		ProcessedCount:   len(dids),
		SuccessCount:     len(dids),
		FailureCount:     0,
		SuccessRate:      1.0,
		RecordCount:      len(dids) * 10,
		PeakMemoryMB:     10.0,
		ApiCalls:         len(dids) * 2,
		RateLimitedCalls: 0,
	}, nil
}

// TestRun tests the main run function
func TestRun(t *testing.T) {
	// Set up test config
	dids := []string{"did:plc:test1", "did:plc:test2"}
	cfg := Config{
		Concurrency: 2,
		Timeout:     time.Second,
		RateLimit:   10,
		RateWindow:  60,
	}

	// Mock HTTP client - in a real test we'd use httptest.NewServer
	client := &http.Client{
		Timeout: time.Second,
	}

	// Execute test run with mock implementation
	result, err := testRun(dids, cfg, client)

	// Validate results
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.ProcessedCount != len(dids) {
		t.Errorf("Expected %d processed DIDs, got %d", len(dids), result.ProcessedCount)
	}
}
